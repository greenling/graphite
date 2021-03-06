require 'eventmachine'
require 'rufus-scheduler'

module Graphite
  class Client

    attr :logger

    # Expects a string in the form of "hostname:port_num" where port_num is optional, and a prefix
    # to identify this server. Example:
    # Graphite::Client.new("graphite.example.com", "yourapp.#{Rails.env}.instances.#{hostname}.#{$$}")
    # valid options are:
    # * graphite_logger - an instance of Graphite::Logger or something that acts like one
    # * logger - a regular logger for writing messages to
    def initialize(server, prefix, options={})
      @logger = options[:graphite_logger] || Graphite::Logger.new(server,options[:logger])
      @prefix = prefix
      @metrics = {}
      @counters = {}
      @shifts = {}

      if block_given?
        @scheduler = Rufus::Scheduler::EmScheduler.start_new
        yield self
        start_logger_timer
        @scheduler.join
      else
        Graphite::EventMachineHandler.ensure_running
        @scheduler = Rufus::Scheduler::EmScheduler.start_new
        start_logger_timer
      end
    end

    def previous_day_metric(name)
      @scheduler.every("1d", :first_in => '1m') do
        date = Date.today - 1
        result = yield date
        log({name + ".daily" => result})
        cleanup
      end
    end

    # Schedules a job according to the supplied cron_string.  The block passed in is expected to return
    # a Hash of {name => value} metric pairs.
    def schedule_job(cron_string, options={}, &block)
      @scheduler.cron(cron_string)  do
        results = yield
        metrics = {}
        results.keys.each do |k,v|
          metrics["#{@prefix}.#{k}"] = results.delete(k)
        end
        @logger.log(Time.now, metrics) if metrics.size > 0
      end
    end

    def metric(name, scheme = 1.minute, options = {})
      add_shifts(name,options[:shifts]) if options[:shifts]

      unless options[:no_immediate]
        result = yield
        log({name => result})
      end

      if scheme.is_a?(Fixnum)
        @scheduler.every(scheme, :first_in => '1m') do
          begin
            result = yield
            log({name => result})
            cleanup
          rescue
          end
        end
      elsif scheme.is_a?(Hash)
        raise "bad scheme value, only 'cron' is supported" unless scheme.keys.length == 1
        how = scheme.keys.first
        time = scheme[how]
        case how
        when :cron
          @scheduler.cron(time) do
            begin
              result = yield
              log({name => result})
              cleanup
            rescue
            end
          end
        else
          raise "unsupported scheduling type: #{how.inspect}"
        end
      else
        raise "bad scheme value #{scheme.inspect}"
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

    def add_shifts(name, shifts)
      shifts.each do |seconds|
        @shifts[seconds] ||= []
        @shifts[seconds] << name
      end
    end

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
      send_shifts
    end

    # Sends metrics into the future
    def send_shifts
      @shifts.each do |time, metrics|
        to_send = {}

        metrics.each do |k|
          key = "#{@prefix}.#{k}"
          shifted_key = key + "_shifted.#{time}_ago"
          to_send[shifted_key] = @metrics[key]
        end

        seconds = Rufus.parse_time_string time
        @logger.log(Time.now + seconds,to_send)
      end
    end

    def start_logger_timer
      @scheduler.every("58s", :blocking => true) do
        send_metrics
      end
    end

    # Blocks get run in a threadpool -- sharing is caring.
    def cleanup
      ActiveRecord::Base.clear_active_connections! if defined?(ActiveRecord::Base)
    end
  end
end
