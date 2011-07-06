require 'rufus-scheduler'

module Graphite
  class Client
    
    # Expects a string in the form of "hostname:port_num" where port_num is optional, and a prefix 
    # to identify this server. Example:
    # Graphite::Client.new("graphite.example.com", "yourapp.#{Rails.env}.instances.#{hostname}.#{$$}")
    # valid options are:
    # * graphite_logger - an instance of Graphite::Logger or something that acts like one
    # * logger - a regular logger for writing messages to
    def initialize(server, prefix, options={})
      @logger = options[:graphite_logger] || Graphite::Logger.new(server,options[:logger])
      @scheduler = options[:scheduler] || Rufus::Scheduler.start_new
      @prefix = prefix
      @metrics = {}
      @counters = {}

      if block_given?
        yield self
        start_logger_timer
        @scheduler.join
      else
        start_logger_timer
      end
    end

    def reset_connection!
      @logger.reset_connection!
    end

    def previous_day_metric(name)
      @daily_metric_offset ||= 0
      @daily_metric_offset += 1
      @scheduler.in(@daily_metric_offset * 60) do
        Rails.logger.info("Running daily #{name}")
        date = Date.today - 1
        result = nil

        begin
          result = yield date
        rescue Exception => e
          logger.error("Caught exception for metric #{name}.daily: #{e}")
        end

        log({name + ".daily" => result}, date.to_time.to_i)
        cleanup
      end

      @scheduler.cron("#{@daily_metric_offset % 60} 1 * * *") do
        Rails.logger.info("Running daily #{name}")
        date = Date.today - 1
        result = nil

        begin
          result = yield date
        rescue Exception => e
          logger.error("Caught exception for metric #{name}.daily: #{e}")
        end

        log({name + ".daily" => result}, date.to_time.to_i)
        cleanup
      end
    end

    def metric(name, frequency = 1.minute, options = {})
      @scheduler.every(frequency, :first_in => '1m') do
        result = yield
        log({name => result})
        cleanup
      end
    end

    def metrics(frequency = 1.minute)
      @scheduler.every(frequency, :first_in => '1m') do
        results = yield
        log(results)
        cleanup
      end
    end

    def increment!(counter, n = 1)
      full_counter = "#{@prefix}.#{counter}"
      @counters[full_counter] ||= 0
      @counters[full_counter]  += n
    end

    private

    def log(results)
      results.keys.each do |k,v|
        @metrics["#{@prefix}.#{k}"] = results.delete(k)
      end
    end

    def send_counters
      to_send = {}
      @counters.keys.each do |k|
        to_send[k] = @counters.delete(k)
      end
      @logger.log(Time.now, to_send) if to_send.size > 0
    end

    def send_metrics
      @logger.log(Time.now, @metrics) if @metrics.size > 0
      send_counters
    end

    def start_logger_timer
      @scheduler.every("60s", :blocking => true) do
        send_metrics
      end
    end

    # Blocks get run in a threadpool -- sharing is caring.
    def cleanup
      ActiveRecord::Base.clear_active_connections! if defined?(ActiveRecord::Base)
    end
  end
end
