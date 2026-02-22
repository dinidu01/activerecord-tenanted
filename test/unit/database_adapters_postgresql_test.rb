# frozen_string_literal: true

require "test_helper"

describe ActiveRecord::Tenanted::DatabaseAdapters::PostgreSQL do
  let(:db_config) do
    Struct.new(:database).new("tenanted_%{tenant}")
  end

  let(:adapter) { described_class.new(db_config) }

  describe "test_workerize" do
    test "appends worker suffix" do
      assert_equal "app_tenant_3", adapter.test_workerize("app_tenant", 3)
    end

    test "does not double-suffix" do
      assert_equal "app_tenant_3", adapter.test_workerize("app_tenant_3", 3)
    end
  end

  describe "path_for" do
    test "returns database identifier unchanged" do
      assert_equal "app_tenant", adapter.path_for("app_tenant")
    end
  end

  describe "validate_tenant_name" do
    test "allows common tenant names" do
      assert_nil adapter.validate_tenant_name("foo-bar_123")
    end

    test "rejects dangerous tenant names" do
      error = assert_raises ActiveRecord::Tenanted::BadTenantNameError do
        adapter.validate_tenant_name("foo'bar")
      end

      assert_includes error.message, "Tenant name contains an invalid character"
    end
  end
end
