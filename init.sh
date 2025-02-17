# Create base project directories
mkdir -p data/marvel/{raw,logs,processed,analytics}/{latest,$(date +%Y%m%d)}
mkdir -p logs

# Set permissions
chmod -R 777 data
chmod -R 777 logs

echo "Initialized directories with correct permissions"