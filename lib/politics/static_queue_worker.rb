# encoding: utf-8
require 'socket'
require 'ipaddr'
require 'uri'
require 'drb'
require 'set'

begin
require 'net/dns/mdns-sd'
require 'net/dns/resolv-mdns'
require 'net/dns/resolv-replace'
rescue LoadError => e
  puts "Unable to load net-mdns, please run `sudo gem install net-mdns`: #{e.message}"
  exit(1)
end

begin
  require 'memcache'
rescue LoadError => e
  puts "Unable to load memcache client, please run `sudo gem install memcache-client`: #{e.message}"
  exit(1)
end

module Politics

  # The StaticQueueWorker mixin allows a processing daemon to "lease" or checkout
  # a portion of a problem space to ensure no other process is processing that same
  # space at the same time.  The processing space is cut into N "buckets", each of
  # which is placed in a queue.  Processes then fetch entries from the queue
  # and process them.  It is up to the application to map the bucket number onto its
  # specific problem space.
  #
  # Note that memcached is used for leader election.  The leader owns the queue during
  # the iteration period and other peers fetch buckets from the current leader during the
  # iteration.
  #
  # The leader hands out buckets in order.  Once all the buckets have been processed, the
  # leader returns nil to the processors which causes them to sleep until the end of the
  # iteration.  Then everyone wakes up, a new leader is elected, and the processing starts
  # all over again.
  #
  # DRb and mDNS are used for peer discovery and communication.
  #
  # Example usage:
  #
  #  class Analyzer
  #    include Politics::StaticQueueWorker
  #    TOTAL_BUCKETS = 16
  #
  #    def start
  #      register_worker(self.class.name, TOTAL_BUCKETS)
  #      process_bucket do |bucket|
  #        puts "Analyzing bucket #{bucket} of #{TOTAL_BUCKETS}"
  #        sleep 5
  #      end
  #    end
  #  end
  #
  # Note: process_bucket never returns i.e. this should be the main loop of your processing daemon.
  #
  module StaticQueueWorker
    attr_reader :group_name, :iteration_length, :dictatorship_length, :uri, :buckets

    # Register this process as able to work on buckets.
    def register_worker(name, bucket_count, config = {})
      options = {
            :iteration_length => 10,
            :servers => ['127.0.0.1:11211']
          }.merge(config)

      @group_name = name
      @iteration_length = options[:iteration_length]
      @memcache_client = client_for(Array(options[:servers]))
      @nominated_at = Time.now
      # FIXME: Tests
      @domain = options[:domain]
      @dictatorship_length = options[:dictatorship_length]

      @buckets = []
      @bucket_count = bucket_count
      @followers_to_stop = Set.new

      register_with_bonjour
      log.progname = uri
      log.info { "Registered in group #{group_name} at port #{@port}" }
      at_exit do
        cleanup
      end
    end

    # Fetch a bucket out of the queue and pass it to the given block to be processed.
    def process_bucket(&block)
      log.debug "start bucket processing"
      raise ArgumentError, "process_bucket requires a block!" unless block_given?
      unless @memcache_client
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
          if leader?
            log.info { "has been elected leader" }
            perform_leader_duties
          else
            # Get a bucket from the leader and process it
            begin
              log.debug "getting bucket request from leader (#{leader_uri}) and processing it"
              bucket_process(*leader.bucket_request(uri), &block)
            rescue DRb::DRbError => dre
              log.error { "Error talking to leader: #{dre.message}" }
              relax until_next_iteration
            end
          end
        rescue MemCache::MemCacheError => e
          log.error { "Unexpected MemCacheError: #{e.message}" }
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

    def seize_leadership(duration = iteration_length)
      @memcache_client.set(token, uri, duration)
      @nominated_at = Time.now + duration - iteration_length
    end

    def perform_leader_duties
      # Drb thread handles requests to leader
      as_dictator do
        initialize_buckets
      end
      # keeping leader state as long as buckets are available by renominating before nomination
      # times out
      while !buckets.empty? do
        log.debug { "relaxes half the time until next iteration" }
        relax(until_next_iteration / 2)
        as_dictator do
          update_buckets unless restart_wanted?
        end
      end
      as_dictator() {populate_followers_to_stop} if restart_wanted?
      # keeping leader state as long as there are followers to stop
      while !followers_to_stop.empty? do
        relax(until_next_iteration / 2)
        seize_leadership
      end
      exit 0 if restart_wanted?
      relax until_next_iteration
    end

    def populate_followers_to_stop
      @followers_to_stop.replace(find_workers)
    end

    def followers_to_stop
      @followers_to_stop.select {|u| DRbObject.new(nil, u).alive? rescue DRb::DRbConnError && false}
    end

    def bucket_request(requestor_uri)
      if leader?
        log.debug "delivering bucket request"
        bucket_spec = next_bucket(requestor_uri)
        if !bucket_spec[0] && @followers_to_stop.include?(requestor_uri)
          bucket_spec = [:stop, 0]
          @followers_to_stop.delete(requestor_uri)
        end
        bucket_spec
      else
        log.debug "received request for bucket but am not leader - delivering :not_leader"
        [:not_leader, 0]
      end
    end

    def next_bucket(requestor_uri)
      [@buckets.pop, sleep_until_next_bucket_time]
    end

    def sleep_until_next_bucket_time
      [[until_next_iteration / 2, 1].max, iteration_length / 2].min
    end

    def until_next_iteration
      left = iteration_length - (Time.now - @nominated_at)
      left > 0 ? left : 0
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
      workers = []
      browser = Net::DNS::MDNSSD.browse(mdns_type) do |reply|
        worker_uri = reply.name.gsub(/#/, '.')
        workers << worker_uri unless worker_uri == uri
      end
      sleep 5
      browser.stop
      workers
    end

    def hostname
      nil
    end

    private

    def restart_wanted?
      @memcache_client.get(restart_flag)
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

    def cleanup
      if leader?
        @memcache_client.delete(token)
        @memcache_client.delete(restart_flag)
      end
    end

    def pause_until_expiry(elapsed)
      pause_time = (iteration_length - elapsed).to_f
      if pause_time > 0
        relax(pause_time)
      else
        raise ArgumentError, "Negative iteration time left. " +
            "Assuming the worst and exiting… #{iteration_length}/#{elapsed}"
      end
    end

    def relax(time)
      sleep time
    end

    # Nominate ourself as leader by contacting the memcached server
    # and attempting to add the token with our name attached.
    def nominate
      log.debug("try to nominate")
      @nominated_at = Time.now
      @memcache_client.add(token, uri, iteration_length)
      @leader_uri = nil
    end

    def leader_uri
      @leader_uri ||= @memcache_client.get(token)
    end

    # Check to see if we are leader by looking at the process name
    # associated with the token.
    def leader?
      until_next_iteration > 0 && uri == leader_uri
    end

    # Easy to mock or monkey-patch if another MemCache client is preferred.
    def client_for(servers)
      MemCache.new(servers)
    end

    def time_for(&block)
      a = Time.now
      yield
      Time.now - a
    end

    def mdns_type
      "_#{group_name}._tcp"
    end

    def register_with_bonjour
      server = DRb.start_service("druby://#{hostname || ""}:0", self)
      @uri = DRb.uri
      @port = URI.parse(DRb.uri).port

      # Register our DRb server with Bonjour.
      name = @uri.gsub(/\./, '#')
      domain = "local"
      log.debug "register service #{name} of type #{mdns_type} within domain #{domain} at port #{@port}"
      handle = Net::DNS::MDNSSD.register(name, mdns_type, domain, @port) do |reply|
        log.debug "registered as #{reply.fullname}"
        if reply.name != name
          log.debug "Registered name #{reply.name} differs from requested name #{name} … exiting."
          handle.stop
        end
      end
    end
  end
end
