require 'socket'

module Graphite
  class Logger
    attr_accessor :logger

    DEFAULT_PORT = 2003

    # Initialize a new Graphite::Logger class; expects a string containing a DNS name for the Graphite server.
    # This hostname may optional include a port number, e.g. "graphite.example.com:3333". If not specified,
    # DEFAULT_PORT will be used. If you specify a Ruby-compatible logger object in the second parameter,
    # a string containing the graphite message will be logged there before it is sent to the socket.

    def initialize(server_host, logger = nil)
      @server = server_host
      @logger = logger
    end

    def socket
      if @socket.nil? || @socket.closed?
        host, port = @server.split(/:/)
        port ||= DEFAULT_PORT
        @socket = TCPSocket.new(host, port)
      end
      @socket
    end

    def reset_connection!
      @socket = nil
    end

    # Write a bunch of values to the server taken at the given time
    def log(measurements)
      message = ""
      measurements.each do |key, ostruct|
        if ostruct.time
          time = ostruct.time
        else
          time = Time.now
        end
        message << "#{key.gsub(" ", "_")} #{ostruct.value.to_f} #{time.to_i}\n"
      end

      logger.info("Graphite: #{message}") if logger

      begin
        socket.write(message)
      rescue Errno::EPIPE, Errno::ECONNRESET, Exception
        @socket = nil
        retry
      end	
    end

  end
end
