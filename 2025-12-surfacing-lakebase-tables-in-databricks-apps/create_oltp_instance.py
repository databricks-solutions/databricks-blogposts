#!/usr/bin/env python3
"""
Script to create a new OLTP database instance called 'your-test-oltp' in Databricks
Based on: https://docs.databricks.com/aws/en/oltp/create/
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import DatabaseInstance

def create_oltp_instance():
    """Create the your-test-oltp database instance"""
    
    print("🚀 Creating 'your-test-oltp' database instance in Databricks...")
    print("=" * 60)
    
    try:
        # Initialize the Workspace client
        print("🔧 Initializing Databricks Workspace client...")
        w = WorkspaceClient()
        print("✅ Workspace client initialized")
        
        # Create a database instance
        print("📊 Creating database instance 'your-test-oltp'...")
        instance = w.database_instances.create_database_instance(
            DatabaseInstance(
                name="your-test-oltp",
                capacity="CU_1"  # Start with smallest capacity
            )
        )
        
        print("✅ Database instance created successfully!")
        print("\n📋 Instance Details:")
        print(f"  • Name: {instance.name}")
        print(f"  • State: {instance.state}")
        print(f"  • Capacity: {instance.capacity}")
        if hasattr(instance, 'host'):
            print(f"  • Host: {instance.host}")
        if hasattr(instance, 'port'):
            print(f"  • Port: {instance.port}")
        if hasattr(instance, 'retention_window_in_days'):
            print(f"  • Retention window: {instance.retention_window_in_days} days")
        
        print("\n🎉 Database instance 'your-test-oltp' setup completed successfully!")
        print("\n💡 Next steps:")
        print("  • Wait for the instance to reach 'AVAILABLE' state")
        print("  • Update your app configuration with the instance details")
        print("  • Test the connection using the test script")
        
        return instance
        
    except Exception as e:
        print(f"❌ Error creating database instance: {e}")
        print("\n🔧 Troubleshooting:")
        print("  • Check your Databricks token permissions")
        print("  • Ensure you have rights to create database instances")
        print("  • Verify your workspace has OLTP enabled")
        return None

def check_instance_status(instance_name="your-test-oltp"):
    """Check the status of the database instance"""
    
    try:
        w = WorkspaceClient()
        instances = w.database_instances.list()
        
        target_instance = None
        for instance in instances:
            if instance.name == instance_name:
                target_instance = instance
                break
        
        if target_instance:
            print(f"✅ Instance '{instance_name}' found!")
            print(f"  • State: {target_instance.state}")
            print(f"  • Capacity: {target_instance.capacity}")
            if hasattr(target_instance, 'host'):
                print(f"  • Host: {target_instance.host}")
            if hasattr(target_instance, 'port'):
                print(f"  • Port: {target_instance.port}")
            return target_instance
        else:
            print(f"❌ Instance '{instance_name}' not found")
            return None
            
    except Exception as e:
        print(f"❌ Error checking instance status: {e}")
        return None

def delete_instance(instance_name="your-test-oltp"):
    """Delete the database instance"""
    
    try:
        w = WorkspaceClient()
        
        print(f"🗑️ Deleting instance '{instance_name}'...")
        w.database_instances.delete(instance_name)
        print(f"✅ Instance '{instance_name}' deleted successfully!")
        
    except Exception as e:
        print(f"❌ Error deleting instance: {e}")

def list_all_instances():
    """List all database instances"""
    
    try:
        w = WorkspaceClient()
        instances = w.database_instances.list()
        
        if instances:
            print("📊 Available database instances:")
            for i, instance in enumerate(instances, 1):
                print(f"  {i}. {instance.name}")
                print(f"     • State: {instance.state}")
                print(f"     • Capacity: {instance.capacity}")
                if hasattr(instance, 'host'):
                    print(f"     • Host: {instance.host}")
                print()
        else:
            print("📭 No database instances found")
            
    except Exception as e:
        print(f"❌ Error listing instances: {e}")

if __name__ == "__main__":
    print("🏗️  Databricks OLTP Database Instance Creator")
    print("Instance Name: your-test-oltp")
    print("Documentation: https://docs.databricks.com/aws/en/oltp/create/")
    print("=" * 80)
    
    # First, list existing instances
    print("\n📋 Checking existing instances...")
    list_all_instances()
    
    # Get existing instances to check for duplicates
    try:
        w = WorkspaceClient()
        existing_instances = list(w.database_instances.list())
    except Exception as e:
        print(f"❌ Error accessing Databricks: {e}")
        exit(1)
    
    # Check if our instance already exists
    instance_exists = any(inst.name == "your-test-oltp" for inst in existing_instances)
    
    if instance_exists:
        print("\n⚠️  Instance 'your-test-oltp' already exists!")
        response = input("Do you want to check its status instead? (y/N): ")
        if response.lower() == 'y':
            check_instance_status()
        else:
            print("Operation cancelled.")
    else:
        print("\n➡️  Proceeding with instance creation...")
        result = create_oltp_instance()
        
        if result:
            print("\n🎯 Setup completed successfully!")
            print("You can now configure your app to use this database instance.") 