# frozen_string_literal: true

require "test_helper"

describe ActiveRecord::Tenanted::DatabaseAdapter do
  describe ".new" do
    test "selects correct adapter for sqlite3" do
      adapter = ActiveRecord::Tenanted::DatabaseAdapter.new(create_config("sqlite3"))
      assert_instance_of ActiveRecord::Tenanted::DatabaseAdapters::SQLite, adapter
    end

    test "selects correct adapter for postgresql" do
      adapter = ActiveRecord::Tenanted::DatabaseAdapter.new(create_config("postgresql"))
      assert_instance_of ActiveRecord::Tenanted::DatabaseAdapters::PostgreSQL, adapter
    end

    test "raises error for unsupported adapter" do
      unsupported_config = create_config("mongodb")

      error = assert_raises ActiveRecord::Tenanted::UnsupportedDatabaseError do
        ActiveRecord::Tenanted::DatabaseAdapter.new(unsupported_config)
      end

      assert_includes error.message, "Unsupported database adapter for tenanting: mongodb."
    end
  end

  private
    def create_config(adapter)
      ActiveRecord::DatabaseConfigurations::HashConfig.new(
        "test",
        "test_config",
        {
          adapter: adapter,
          database: "db_name",
        }
      )
    end
end
