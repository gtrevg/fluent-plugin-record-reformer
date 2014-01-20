require 'socket'

module Fluent
  class RecordReformerOutput < Output
    Fluent::Plugin.register_output('record_reformer', self)

    def initialize
      super
    end

    config_param :output_tag, :string
    config_param :remove_keys, :string, :default => nil
    config_param :renew_record, :bool, :default => false
    config_param :enable_ruby, :bool, :default => true # true for lower version compatibility

    BUILTIN_CONFIGURATIONS = %W(type output_tag remove_keys renew_record enable_ruby)

    def configure(conf)
      super

      @map = {}
      conf.each_pair { |k, v|
        next if BUILTIN_CONFIGURATIONS.include?(k)
        conf.has_key?(k) # to suppress unread configuration warning
        @map[k] = v
      }
      # <record></record> directive
      conf.elements.select { |element| element.name == 'record' }.each { |element|
        element.each_pair { |k, v|
          element.has_key?(k) # to suppress unread configuration warning
          @map[k] = v
        }
      }

      if @remove_keys
        @remove_keys = @remove_keys.split(',')
      end

      @hostname = Socket.gethostname

      @placeholder_expander =
        if @enable_ruby
          # require utilities which would be used in ruby placeholders
          require 'pathname'
          require 'uri'
          require 'cgi'
          RubyPlaceholderExpander.new(:hostname => @hostname)
        else
          PlaceholderExpander.new(:hostname => @hostname)
        end
    end

    def emit(tag, es, chain)
      @placeholder_expander.prepare_tag_placeholders(tag)
      es.each { |time, record|
        @placeholder_expander.prepare_event_placeholders(time, record)
        new_tag, new_record = reform(@output_tag, record)
        Engine.emit(new_tag, time, new_record)
      }
      chain.next
    rescue => e
      $log.warn "record_reformer: #{e.class} #{e.message} #{e.backtrace.first}"
    end

    private

    def reform(output_tag, record)
      new_tag = @placeholder_expander.expand(output_tag)

      new_record = @renew_record ? {} : record.dup
      @map.each_pair { |k, v| new_record[k] = @placeholder_expander.expand(v) }
      @remove_keys.each { |k| new_record.delete(k) } if @remove_keys

      [new_tag, new_record]
    end

    module TagHelper
      def tag_prefix(tag_parts)
        return [] if tag_parts.empty?
        tag_prefix = [tag_parts.first]
        1.upto(tag_parts.size-1).each do |i|
          tag_prefix[i] = "#{tag_prefix[i-1]}.#{tag_parts[i]}"
        end
        tag_prefix
      end

      def tag_suffix(tag_parts)
        return [] if tag_parts.empty?
        rev_tag_parts = tag_parts.reverse
        rev_tag_suffix = [rev_tag_parts.first]
        1.upto(tag_parts.size-1).each do |i|
          rev_tag_suffix[i] = "#{rev_tag_parts[i]}.#{rev_tag_suffix[i-1]}"
        end
        rev_tag_suffix.reverse
      end
    end

    class PlaceholderExpander
      include TagHelper
      attr_reader :placeholders

      def initialize(hash)
        @placeholders = {}
        hash.each { |k, v| @placeholders.store("${#{k}}", v) }
      end

      def prepare_tag_placeholders(tag)
        tag_parts  = tag.split('.')
        tag_prefix = tag_prefix(tag_parts)
        tag_suffix = tag_suffix(tag_parts)

        placeholders = {
          '${tag}' => tag,
        }

        size = tag_parts.size

        tag_parts.each_with_index { |t, idx|
          placeholders.store("${tag_parts[#{idx}]}", t)
          placeholders.store("${tag_parts[#{idx-size}]}", t) # support tag_parts[-1]

          # tags is just for old version compatibility
          placeholders.store("${tags[#{idx}]}", t)
          placeholders.store("${tags[#{idx-size}]}", t) # support tags[-1]
        }

        tag_prefix.each_with_index { |t, idx|
          placeholders.store("${tag_prefix[#{idx}]}", t)
          placeholders.store("${tag_prefix[#{idx-size}]}", t) # support tag_prefix[-1]
        }

        tag_suffix.each_with_index { |t, idx|
          placeholders.store("${tag_suffix[#{idx}]}", t)
          placeholders.store("${tag_suffix[#{idx-size}]}", t) # support tag_suffix[-1]
        }

        @placeholders.merge!(placeholders)
      end

      def prepare_event_placeholders(time, record)
        placeholders = {
          '${time}' => Time.at(time).to_s,
        }

        record.each { |k, v|
          placeholders.store("${#{k}}", v)
        }

        @placeholders.merge!(placeholders)
      end

      def expand(str)
        str.gsub(/(\${[a-z_]+(\[-?[0-9]+\])?}|__[A-Z_]+__)/) {
          $log.warn "record_reformer: unknown placeholder `#{$1}` found" unless @placeholders.include?($1)
          @placeholders[$1]
        }
      end
    end

    class RubyPlaceholderExpander
      include TagHelper
      attr_reader :placeholders

      def initialize(hash)
        @placeholders = UndefOpenStruct.new(hash)
      end

      # Get placeholders as a struct
      #
      # @param [String] tag         the tag
      def prepare_tag_placeholders(tag)
        tag_parts  = tag.split('.')
        tag_prefix = tag_prefix(tag_parts)
        tag_suffix = tag_suffix(tag_parts)
        @placeholders.tag  = tag
        @placeholders.tags = @placeholders.tag_parts = tag_parts # tags is for old version compatibility
        @placeholders.tag_prefix = tag_prefix
        @placeholders.tag_suffix = tag_suffix
      end

      # Get event stream (time, record) placeholders as a struct
      #
      # @param [Time]   time        the time
      # @param [Hash]   record      the record
      def prepare_event_placeholders(time, record)
        @placeholders.time = Time.at(time)
        record.each { |k, v| @placeholders.__send__("#{k}=", v) }
      end

      # Replace placeholders in a string
      #
      # @param [String] str         the string to be replaced
      def expand(str)
        str = str.gsub(/\$\{([^}]+)\}/, '#{\1}') # ${..} => #{..}
        eval "\"#{str}\"", @placeholders.instance_eval { binding }
      end

      class UndefOpenStruct < OpenStruct
        (Object.instance_methods).each do |m|
          undef_method m unless m.to_s =~ /^__|respond_to_missing\?|object_id|public_methods|instance_eval|method_missing|define_singleton_method|respond_to\?|new_ostruct_member/
        end
      end
    end
  end
end
