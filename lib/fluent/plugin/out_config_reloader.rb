#
# Copyright 2018- Isuranga Perera
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'rest_client'

module Fluent
  class Fluent::ConfigReloaderOutput < Fluent::MultiOutput
    Fluent::Plugin.register_output('config_reloader', self)

    config_param :config_file, :string
    config_param :continuous_reload, :bool, default: false
    config_param :reload_interval, :integer, default: 1
    config_param :log_type, :string
    config_param :remote_config, :bool, default: true
    config_param :remote_server, :string

    class ReloadFileWatcher
      require 'observer'
      include ::Observable

      attr_reader :thread
      # @param [Object] observer
      # @param [Object] watch_file
      # @param [Object] interval
      def self.create(observer, interval)
        obj = new
        obj.add_observer(observer)
        obj.watch interval

        obj
      end

      def watch(interval)
        @thread = Thread.new do
          loop do
            sleep interval
            notify_observers
          end
        end
      end
    end

    # @return [Object]
    def initialize
      super

      @q = Queue.new
      @log_status = 'on'
      @log_level = 'info'
    end

    # @return [Object]
    def outputs
      [@output]
    end

    # @param [Object] conf
    # @return [Object]
    def configure(conf)
      super

      load_config_file(true)
    end

    # @return [Object]
    def start
      output_start
      @thread = Thread.new(&method(:run))

      if @continuous_reload
        @watcher = ReloadFileWatcher.create(self, @reload_interval)
      end
    rescue StandardError
      $log.warn "raises exception: #{$ERROR_INFO.class}, '#{$ERROR_INFO.message}"
    end

    # @return [Object]
    def shutdown
      @watcher.delete_observers
      Thread.kill(@thread)
      output_shutdown
    rescue StandardError
      $log.warn "raises exception: #{$ERROR_INFO.class}, '#{$ERROR_INFO.message}"
    end

    # @param [Object] tag
    # @param [Object] es
    # @param [Object] chain
    def emit(tag, es, chain)
      param = OpenStruct.new
      param.tag = tag
      param.es = es
      param.chain = chain

      @q.push param
    end

    # @return [Object]
    def update
      $log.warn 'config_reloader: reload config file start'
      load_config_file(false)
      $log.warn 'config_reloader: reload config file end'
    end

    private

    def output_start
      @output.start
    end

    def output_shutdown
      @output.shutdown
    end

    def load_config_file(_is_init)
      if @remote_config
        handle_remote_config_on(_is_init)
      else
        handle_in_start
      end
    end

    def handle_remote_config_on(is_init)
      response = remote_config
      handle_in_start if response == NIL

      configs = JSON.parse(response)

      unless configs[@log_type] == NIL
        log_status = if configs[@log_type][:status] == NIL
                       @log_status
                     else
                       configs[@log_type][:status]
                     end
        log_level = if configs[@log_type][:level] == NIL
                      @log_level
                    else
                      configs[@log_type][:level]
                    end
      end

      handle_in_start if is_init && log_status.casecmp('on').zero?

      unless is_init
        if(log_status != @log_status)
          case log_status
            when 'on' then handle_in_start
            when 'off' then output_shutdown
          end
        end
      end

    end

    def handle_in_start
      store_elements = parse_config
      handle_store_definition(store_elements.size)

      store_element = store_elements.first
      type = store_element['type']
      unless type
        raise ConfigError, "Missing 'type' parameter on <store> directive"
      end
      log.debug "adding store type=#{type.dump}"

      @output = Plugin.new_output(type)
      @output.configure(store_element)

      output_start
    end

    def remote_config
      RestClient.get(@remote_server) do |response, _request, _result|
        case response.code
        when 200
          log.debug 'Fetched configs from remote server'
          return response.body
        else
          return NIL
        end
      end
    end

    def parse_config
      path = File.expand_path(@config_file)

      store_elements = File.open(path) do |io|
        if File.extname(path) == '.rb'
          require 'fluent/config/dsl'
          Config::DSL::Parser.parse(io, path)
        else
          Config.parse(io, File.basename(path), File.dirname(path), false)
        end
      end.elements.select { |e| e.name == 'store' }

      store_elements
    end

    def handle_store_definition(stores)
      no_store_error_msg = '<store> directives are not available'
      multiple_stores_error_msg = 'Multiple <store> directives are not available'

      raise ConfigError, no_store_error_msg if stores.zero?
      raise ConfigError, multiple_stores_error_msg if stores > 1
    end

    def run
      loop do
        param = @q.pop

        tag = param.tag
        es = param.es
        chain = param.chain

        begin
          unless es.repeatable?
            m = MultiEventStream.new
            es.each do |time, record|
              m.add(time, record)
            end
            es = m
          end
          chain = OutputChain.new([@output], tag, es, chain)
          chain.next
        rescue StandardError
          $log.warn "raises exception: #{$ERROR_INFO.class}, '#{$ERROR_INFO.message}, #{param}'"
        end
      end
    end

  end
end
