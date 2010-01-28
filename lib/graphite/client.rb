module Graphite
  class Client
    def initialize(server, prefix)
      @logger = Graphite::Logger.new(server)
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
        EventMachineHandler.ensure_running
        @scheduler = Rufus::Scheduler::EmScheduler.start_new
        start_logger_timer
      end
    end

    def metric(name, frequency = 1.minute, options = {})
      add_shifts(name,options[:shifts]) if options[:shifts]
      @scheduler.every(frequency, :first_in => '1m') do
        result = yield
        log({name => result})
        cleanup
      end
    end

    def add_shifts(name, shifts)
      shifts.each do |seconds|
        @shifts[seconds] ||= []
        @shifts[seconds] << name
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
      @scheduler.every(60.seconds, :blocking => true) do
        send_metrics
      end
    end

    # Blocks get run in a threadpool -- sharing is caring.
    def cleanup
      ActiveRecord::Base.clear_active_connections! if defined?(ActiveRecord::Base)
    end
  end
end