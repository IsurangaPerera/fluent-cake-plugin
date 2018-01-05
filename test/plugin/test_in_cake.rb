require "helper"
require "fluent/plugin/in_cake.rb"

class CakeInputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  test "failure" do
    flunk
  end

  private

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::CakeInput).configure(conf)
  end
end
