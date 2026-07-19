-- SHIFT Hub Database Schema
-- PostgreSQL database schema for the Hub services

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Function to update updated_at timestamp (must be created before triggers)
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Accounts table
CREATE TABLE accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    billing_profile_type VARCHAR(50) NOT NULL CHECK (billing_profile_type IN ('Self-Hosted', 'Cloud Managed', 'Hosted Arrears')),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_accounts_created_at ON accounts(created_at);

-- Users table
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL UNIQUE,
    oidc_subject VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_account_id ON users(account_id);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_oidc_subject ON users(oidc_subject);

-- Roles table
CREATE TABLE roles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    permissions JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(account_id, name)
);

CREATE INDEX idx_roles_account_id ON roles(account_id);

-- User roles junction table
CREATE TABLE user_roles (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_id UUID NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    assigned_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, role_id)
);

CREATE INDEX idx_user_roles_user_id ON user_roles(user_id);
CREATE INDEX idx_user_roles_role_id ON user_roles(role_id);

-- Runner groups table
CREATE TABLE runner_groups (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(account_id, name)
);

CREATE INDEX idx_runner_groups_account_id ON runner_groups(account_id);

-- Runners table
CREATE TABLE runners (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    runner_group_id UUID NOT NULL REFERENCES runner_groups(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    registration_token VARCHAR(255) NOT NULL UNIQUE,
    api_key_hash VARCHAR(255),
    status VARCHAR(50) NOT NULL DEFAULT 'offline' CHECK (status IN ('online', 'offline', 'error')),
    hostname VARCHAR(255),
    p2p_port INTEGER,
    last_seen_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_runners_runner_group_id ON runners(runner_group_id);
CREATE INDEX idx_runners_status ON runners(status);
CREATE INDEX idx_runners_registration_token ON runners(registration_token);

-- Runner group configuration table
CREATE TABLE runner_group_configs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    runner_group_id UUID NOT NULL REFERENCES runner_groups(id) ON DELETE CASCADE,
    config JSONB NOT NULL,
    group_secret_hash VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(runner_group_id, version)
);

CREATE INDEX idx_runner_group_configs_group_id ON runner_group_configs(runner_group_id);
CREATE INDEX idx_runner_group_configs_version ON runner_group_configs(version);

-- Runner group status table
CREATE TABLE runner_group_status (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    runner_group_id UUID NOT NULL REFERENCES runner_groups(id) ON DELETE CASCADE,
    reported_by_runner_id UUID REFERENCES runners(id) ON DELETE SET NULL,
    status JSONB NOT NULL,
    health_score INTEGER CHECK (health_score >= 0 AND health_score <= 100),
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_runner_group_status_group_id ON runner_group_status(runner_group_id);
CREATE INDEX idx_runner_group_status_recorded_at ON runner_group_status(recorded_at);

-- Integration flows table (with versioning)
CREATE TABLE integration_flows (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    runner_group_id UUID REFERENCES runner_groups(id) ON DELETE SET NULL,
    name VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    definition JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'published', 'archived', 'deployed')),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(account_id, name, version)
);

CREATE INDEX idx_integration_flows_account_id ON integration_flows(account_id);
CREATE INDEX idx_integration_flows_status ON integration_flows(status);
CREATE INDEX idx_integration_flows_name_version ON integration_flows(name, version);

-- Connectors catalog table (with versioning)
CREATE TABLE connectors (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    version VARCHAR(50) NOT NULL,
    connector_type VARCHAR(100) NOT NULL CHECK (connector_type IN ('OpenAPI', 'HTTP', 'Database', 'SFTP', 'Local Disk', 'Xero', 'QuickBooks', 'Shopify', 'Custom')),
    definition JSONB NOT NULL,
    binary_url VARCHAR(512),
    checksum VARCHAR(255),
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(name, version)
);

CREATE INDEX idx_connectors_name_version ON connectors(name, version);
CREATE INDEX idx_connectors_type ON connectors(connector_type);
CREATE INDEX idx_connectors_is_active ON connectors(is_active);

-- Billing profiles table
CREATE TABLE billing_profiles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    profile_type VARCHAR(50) NOT NULL CHECK (profile_type IN ('Self-Hosted', 'Cloud Managed', 'Hosted Arrears')),
    billing_cycle VARCHAR(50) NOT NULL DEFAULT 'monthly' CHECK (billing_cycle IN ('monthly', 'quarterly', 'annually')),
    rate_per_execution DECIMAL(10, 6),
    rate_per_api_call DECIMAL(10, 6),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(account_id)
);

CREATE INDEX idx_billing_profiles_account_id ON billing_profiles(account_id);

-- Usage metrics table
CREATE TABLE usage_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    runner_id UUID REFERENCES runners(id) ON DELETE SET NULL,
    flow_id UUID REFERENCES integration_flows(id) ON DELETE SET NULL,
    metric_type VARCHAR(50) NOT NULL CHECK (metric_type IN ('flow_execution', 'api_call', 'connector_usage')),
    metric_value DECIMAL(15, 2) NOT NULL,
    metadata JSONB DEFAULT '{}',
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_usage_metrics_account_id ON usage_metrics(account_id);
CREATE INDEX idx_usage_metrics_runner_id ON usage_metrics(runner_id);
CREATE INDEX idx_usage_metrics_flow_id ON usage_metrics(flow_id);
CREATE INDEX idx_usage_metrics_recorded_at ON usage_metrics(recorded_at);
CREATE INDEX idx_usage_metrics_type ON usage_metrics(metric_type);

-- Integration execution tasks table
CREATE TABLE integration_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    flow_id UUID NOT NULL REFERENCES integration_flows(id) ON DELETE CASCADE,
    runner_id UUID REFERENCES runners(id) ON DELETE SET NULL,
    runner_group_id UUID REFERENCES runner_groups(id) ON DELETE SET NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'running', 'completed', 'failed', 'cancelled')),
    input_payload JSONB,
    output_payload JSONB,
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_ms INTEGER,
    cpu_time_ms INTEGER,
    memory_used_mb INTEGER,
    connectors_used JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_integration_executions_account_id ON integration_executions(account_id);
CREATE INDEX idx_integration_executions_flow_id ON integration_executions(flow_id);
CREATE INDEX idx_integration_executions_runner_id ON integration_executions(runner_id);
CREATE INDEX idx_integration_executions_status ON integration_executions(status);
CREATE INDEX idx_integration_executions_created_at ON integration_executions(created_at);

-- Triggers to automatically update updated_at
CREATE TRIGGER update_integration_executions_updated_at BEFORE UPDATE ON integration_executions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_accounts_updated_at BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_roles_updated_at BEFORE UPDATE ON roles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_runner_groups_updated_at BEFORE UPDATE ON runner_groups
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_runners_updated_at BEFORE UPDATE ON runners
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_integration_flows_updated_at BEFORE UPDATE ON integration_flows
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_connectors_updated_at BEFORE UPDATE ON connectors
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_billing_profiles_updated_at BEFORE UPDATE ON billing_profiles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_runner_group_configs_updated_at BEFORE UPDATE ON runner_group_configs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

