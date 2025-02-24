PROJECT_NAME = local-dynamodb-tester
VERSION = 1.0
JAR_FILE = build/libs/$(PROJECT_NAME)-$(VERSION).jar
SAMPLE_FILE = src/main/resources/sample-data.json
TABLE_NAME = common_data_service_poc_2
AWS_PROFILE = dev  #  Default profile (can be overridden)

.PHONY: all clean build run

# Initialize Gradle Wrapper
init:
	gradle wrapper

# Clean previous builds
clean:
	./gradlew clean

# Build the project
build:
	./gradlew build

# Run the application with sample data and AWS profile
run: build
	java -jar $(JAR_FILE) $(SAMPLE_FILE) $(TABLE_NAME) $(AWS_PROFILE)

# Run in debug mode (useful for debugging)
debug: build
	java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005 -jar $(JAR_FILE) $(SAMPLE_FILE) $(TABLE_NAME) $(AWS_PROFILE)
