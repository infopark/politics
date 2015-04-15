# encoding: utf-8
require 'socket'
require 'ipaddr'
require 'uri'
require 'drb'
require 'set'
require 'logger'
require 'dalli'

module Politics
  module StaticQueueWorker
    attr_reader :group_name, :iteration_length, :dictatorship_length, :uri, :buckets

    # Register this process as able to work on buckets.
    def register_worker(name, bucket_count, config = {})
      options = config.dup
      options[:iteration_length] ||= 10
      options[:servers] ||= ['127.0.0.1:11211']

      @group_name = name
      @iteration_length = options[:iteration_length]
      @memcache_client = client_for(Array(options[:servers]))
      @dictatorship_length = options[:dictatorship_length]

      @buckets = []
      @bucket_count = bucket_count
      @followers_to_stop = Set.new

      start_druby_service
      log.progname = uri
      log.info { "Registered in group #{group_name} at #{uri}" }
      at_exit do
        internal_cleanup
        cleanup
      end
    end

    # Fetch a bucket out of the queue and pass it to the given block to be processed.
    def process_bucket(&block)
      log.debug "start bucket processing"
      raise ArgumentError, "process_bucket requires a block!" unless block_given?
      unless memcache_client
        raise ArgumentError, "You must call register_worker before processing!"
      end

      begin
        begin
          raise "self is not alive via drb" unless DRbObject.new(nil, uri).alive?
        rescue Exception => e
          raise "cannot reach self via drb: #{e.message}"
        end
        begin
          nominate

          if leader? && !(@leader_thread && @leader_thread.alive?)
            unless (@leader_thread && @leader_thread.alive?)
              @leader_thread = Thread.new do
                perform_leader_duties
              end
            end
          end

          # Get a bucket from the leader and process it
          begin
            log.debug "getting bucket request from leader (#{leader_uri}) and processing it"
            bucket_process(*leader.bucket_request(uri, bucket_request_context), &block)
          rescue DRb::DRbError => dre
            log.error { "Error talking to leader: #{dre.message}" }
            relax until_next_iteration
          end
        rescue Dalli::DalliError => e
          log.error { "Unexpected DalliError: #{e.message}" }
          relax until_next_iteration
        end
      end while loop?
    end

    def as_dictator(&block)
      duration = dictatorship_length || (iteration_length * 10)
      log.debug { "become dictator for up to #{duration} seconds" }
      seize_leadership duration
      yield
      raise "lost leadership while being dictator for #{duration} seconds" unless leader?
      seize_leadership
    end

    def seize_leadership(*args)
      start_iteration(*args) {|duration| memcache_client.set(token, uri, duration) }
    end

    def perform_leader_duties
      # The DRb thread handles the requests to the leader.
      # This method performs the bucket managing.
      log.info { "has been elected leader" }
      before_perform_leader_duties
      # keeping leader state as long as buckets are being initialized
      as_dictator { initialize_buckets }

      while !buckets.empty?
        # keeping leader state as long as buckets are available by renominating before
        # nomination times out
        as_dictator { update_buckets } unless restart_wanted?
      end

      if restart_wanted?
        log.info "restart triggered"
        as_dictator { populate_followers_to_stop }
        # keeping leader state as long as there are followers to stop
        while !followers_to_stop.empty?
          log.info "waiting fo workers to stop: #{followers_to_stop}"
          relax(until_next_iteration / 2)
          seize_leadership
        end
        log.info "leader exiting due to trigger"
        exit 0
      end
    rescue Exception => e
      log.error("error while performing leader duties: #{e}\n#{e.backtrace.join("\n")}")
    end

    def populate_followers_to_stop
      @followers_to_stop.replace(find_workers)
    end

    def followers_to_stop
      @followers_to_stop.select {|u| DRbObject.new(nil, u).alive? rescue DRb::DRbConnError && false}
    end

    def bucket_request(requestor_uri, context)
      if leader?
        log.debug "delivering bucket request"
        bucket_spec = next_bucket(requestor_uri, context)
        if !bucket_spec[0] && @followers_to_stop.include?(requestor_uri)
          # the leader stops its own process and must not be killed by its worker
          if requestor_uri != uri
            bucket_spec = [:stop, 0]
          end
          @followers_to_stop.delete(requestor_uri)
        end
        bucket_spec
      else
        log.debug "received request for bucket but am not leader - delivering :not_leader"
        [:not_leader, 0]
      end
    end

    def next_bucket(requestor_uri, context)
      [@buckets.pop, sleep_until_next_bucket_time]
    end

    def sleep_until_next_bucket_time
      [[until_next_iteration / 2, 1].max, iteration_length / 2].min
    end

    def until_next_iteration
      [(iteration_end || Time.at(0)) - Time.now, 0].max
    end

    def alive?
      true
    end

    def leader
      2.times do
        break if leader_uri
        log.debug "could not determine leader - relaxing until next iteration"
        relax until_next_iteration
      end
      raise "cannot determine leader" unless leader_uri
      DRbObject.new(nil, leader_uri)
    end

    def find_workers
      raise "Please provide a method ”find_workers” returning a list of all other worker URIs"
    end

    def restart_wanted?
      memcache_client.get(restart_flag)
    end

    private

    attr_reader :iteration_end, :memcache_client

    def set_iteration_end(interval = iteration_length)
      @iteration_end = Time.now + interval
    end

    def before_perform_leader_duties
    end

    def bucket_process(bucket, sleep_time)
      case bucket
      when nil
        # No more buckets to process this iteration
        log.info { "No more buckets in this iteration, sleeping for #{sleep_time} sec" }
        sleep sleep_time
      when :not_leader
        # Uh oh, race condition?  Invalid any local cache and check again
        log.warn { "Recv'd NOT_LEADER from peer." }
        relax 1
        @leader_uri = nil
      when :stop
        log.info "Received STOP from leader … exiting."
        exit 0
      else
        log.info { "processing #{bucket}" }
        yield bucket
      end
    end

    def log
      @logger ||= Logger.new(STDOUT)
    end

    def initialize_buckets
      @buckets.clear
      @bucket_count.times { |idx| @buckets << idx }
    end

    def update_buckets
      log.debug { "relaxes half the time until next iteration" }
      relax(until_next_iteration / 2)
    end

    def loop?
      true
    end

    def token
      "#{group_name}_token"
    end

    def restart_flag
      "#{group_name}_restart"
    end

    def internal_cleanup
      log.debug("uri: #{uri}, leader_uri: #{leader_uri}, until_next_iteration: #{until_next_iteration}")
      if leader?
        memcache_client.delete(token)
        memcache_client.delete(restart_flag)
      end
    end

    def cleanup
      # for custom use
    end

    def relax(time)
      sleep time
    end

    # Nominate ourself as leader by contacting the memcached server
    # and attempting to add the token with our name attached.
    def nominate
      log.debug("try to nominate")
      start_iteration {|duration| memcache_client.add(token, uri, duration) }
    end

    def start_iteration(duration = iteration_length)
      yield duration
      set_iteration_end(duration)
      @leader_uri = nil
    end

    def leader_uri
      @leader_uri ||= memcache_client.get(token)
    end

    # Check to see if we are leader by looking at the process name
    # associated with the token.
    def leader?
      until_next_iteration > 0 && uri == leader_uri
    end

    # Easy to mock or monkey-patch if another MemCache client is preferred.
    def client_for(servers)
      Dalli::Client.new(servers)
    end

    def time_for(&block)
      a = Time.now
      yield
      Time.now - a
    end

    def bucket_request_context
    end

    def hostname
    end

    def register_service
    end

    def start_druby_service
      @uri = DRb.start_service("druby://#{hostname || ""}:0", self).uri
    end
  end
end
