#!/bin/bash

# Create and schedule a test integration that runs every minute
# The integration does an HTTP GET and sleeps for 90 seconds

HUB_URL="http://localhost:2000"

echo "Creating scheduled integration..."

curl -X POST "${HUB_URL}/api/integrations/create-test" \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "account-1",
    "name": "http-test",
    "schedule": "0 * * * * *"
  }' | jq '.'

echo ""
echo "Integration created and scheduled to run every minute!"
echo "The integration will:"
echo "  1. Do HTTP GET to https://gogogogogogogogogogo.free.beeceptor.com"
echo "  2. Sleep for 90 seconds"
echo ""
echo "Since executions run for 90 seconds and schedule is every 60 seconds,"
echo "you should see that the second execution goes to a different runner!"


