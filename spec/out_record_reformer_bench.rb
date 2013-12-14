# encoding: UTF-8
require_relative 'spec_helper'

Fluent::Test.setup
require 'benchmark'

def time
  Time.now.to_i
end

def message
  "2013/01/13T07:02:11.124202 INFO GET /ping"
end

def tag
  'foo.bar'
end

def create_driver(config)
  Fluent::Test::OutputTestDriver.new(Fluent::RecordReformerOutput, tag).configure(config)
end

# cf. fluent-plugin-grep
#              user     system      total        real
#          0.040000   0.000000   0.040000 (  0.559926)

def bench1
  driver = create_driver(%[
    output_tag reformed.${tag}

    hostname ${hostname}
    tag ${tag}
    time ${time.strftime('%Y-%m-%dT%H:%M:%S%z')}
    message ${hostname} ${tag_parts.last} ${message}
  ])

  n = 1000
  Benchmark.bm(7) do |x|
    x.report { driver.run { n.times { driver.emit({'message' => message}, time) } } }
  end
  # 0.1.1
  #              user     system      total        real
  #          0.950000   0.020000   0.970000 (  1.528941)
end

def bench2
  driver = create_driver(%[
    output_tag reformed.${tag}

    hostname ${hostname}
    tag ${tag}
    message ${hostname} ${tag_parts.last} ${message}
  ])

  n = 1000
  Benchmark.bm(7) do |x|
    x.report { driver.run { n.times { driver.emit({'message' => message}, time) } } }
  end
  # 0.1.1
  #              user     system      total        real
  #          0.590000   0.000000   0.590000 (  1.130823)
end

bench1
bench2
