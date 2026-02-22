# frozen_string_literal: true

require "tmpdir"

module ActiveRecord
  module Tenanted
    module DatabaseAdapters # :nodoc:
      class PostgreSQL
        attr_reader :db_config

        def initialize(db_config)
          @db_config = db_config
        end

        def tenant_databases
          scanner = tenant_name_scanner

          with_maintenance_connection do |connection|
            connection.select_values("SELECT datname FROM pg_database WHERE datistemplate = false").filter_map do |name|
              name.match(scanner)&.captures&.first
            end
          end
        end

        def validate_tenant_name(tenant_name)
          if tenant_name.match?(%r{[/'"`]})
            raise BadTenantNameError, "Tenant name contains an invalid character: #{tenant_name.inspect}"
          end
        end

        def create_database
          ActiveRecord::Tasks::DatabaseTasks.create(db_config)
        end

        def drop_database
          ActiveRecord::Tasks::DatabaseTasks.drop(db_config)
        rescue ActiveRecord::NoDatabaseError
          nil
        end

        def database_exist?
          with_maintenance_connection do |connection|
            database = connection.quote(db_config.database.to_s)
            connection.select_value("SELECT 1 FROM pg_database WHERE datname = #{database} LIMIT 1").present?
          end
        rescue ActiveRecord::NoDatabaseError
          false
        end

        def database_ready?
          database_exist? && !ActiveRecord::Tenanted::Mutex::Ready.locked?(database_path)
        end

        def acquire_ready_lock(&block)
          ActiveRecord::Tenanted::Mutex::Ready.lock(database_path, &block)
        end

        def ensure_database_directory_exists
          # no-op for server-backed databases
        end

        def database_path
          File.join(Dir.tmpdir, "active_record_tenanted", "postgresql", db_config.database.to_s)
        end

        def test_workerize(db, test_worker_id)
          test_worker_suffix = "_#{test_worker_id}"
          db.end_with?(test_worker_suffix) ? db : db + test_worker_suffix
        end

        def path_for(database)
          database
        end

        private
          def tenant_name_scanner
            pattern = Regexp.escape(db_config.database.to_s).gsub("%\\{tenant\\}", "(.+)")
            Regexp.new("\\A#{pattern}\\z")
          end

          def maintenance_db
            db_config.configuration_hash[:maintenance_db].presence || "postgres"
          end

          def maintenance_config
            config_hash = db_config.configuration_hash.merge(database: maintenance_db)
            ActiveRecord::DatabaseConfigurations::HashConfig.new(
              db_config.env_name,
              "#{db_config.name}_maintenance",
              config_hash
            )
          end

          def with_maintenance_connection(&block)
            ActiveRecord::Tasks::DatabaseTasks.with_temporary_connection(maintenance_config, &block)
          end
      end
    end
  end
end
