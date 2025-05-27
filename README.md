# Zenith Trader

Real-Time Volatility Alert and Stop Order Execution System for Stocks

## Overview

Zenith Trader is a reactive application built with Spring Boot that provides real-time monitoring of stock prices, volatility detection, and automated stop order execution. The system allows traders to create stop-loss and stop-gain orders that are automatically triggered when specific price conditions are met.

## Features

- **Real-time Price Monitoring**: Simulates real-time price ticks for configured stock symbols
- **Volatility Detection**: Identifies significant price movements and generates alerts
- **Stop Order Management**: Create, retrieve, and cancel stop orders
- **Automated Order Execution**: Automatically triggers orders when price conditions are met
- **Real-time Notifications**: Server-Sent Events (SSE) for real-time updates to clients
- **Metrics and Monitoring**: Prometheus integration for system metrics

## Architecture

The application follows a reactive architecture using Spring WebFlux, with the following components:

- **API Layer**: RESTful endpoints for order management and SSE for notifications
- **Service Layer**: Business logic for order management, price simulation, and volatility detection
- **Data Layer**: MongoDB for persistent storage and Redis for caching active orders
- **Monitoring**: Prometheus and Grafana for metrics collection and visualization

### Technology Stack

- **Java 21**: Latest LTS version of Java
- **Spring Boot 3.5.0**: Framework for building reactive applications
- **Spring WebFlux**: Reactive web framework
- **Spring Data MongoDB Reactive**: Reactive MongoDB driver
- **Spring Data Redis Reactive**: Reactive Redis driver
- **Docker & Docker Compose**: Containerization and orchestration
- **Prometheus & Grafana**: Monitoring and visualization

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- Java 21 (for local development)
- Maven (for local development)

### Running with Docker Compose

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd zenith-trader
   ```

2. Start the application and its dependencies:
   ```bash
   docker-compose up -d
   ```

3. Access the application at http://localhost:8080
4. Access Prometheus at http://localhost:9090
5. Access Grafana at http://localhost:3000 (username: admin, password: admin)

### Running Locally

1. Start MongoDB and Redis (using Docker or locally)
2. Update application.yaml with the correct connection details
3. Run the application:
   ```bash
   ./mvnw spring-boot:run
   ```

## API Documentation

### Postman Collection

A Postman collection is provided in the file `zenith-trader-postman-collection.json` at the root of the project. This collection includes all API endpoints and test scenarios to help you quickly test the application.

To use the collection:
1. Import the file into Postman
2. Update the `orderId` variable with an actual order ID after creating an order
3. Run the requests individually or use the "Test Scenarios" folder for common test cases

### Stop Order Endpoints

#### Create a Stop Order

- **URL**: `/api/v1/orders`
- **Method**: `POST`
- **Request Body**:
  ```json
  {
    "userId": "user123",
    "stockSymbol": "AAPL",
    "triggerPrice": 150.00,
    "type": "STOP_LOSS",
    "quantity": 10
  }
  ```
- **Response**: The created stop order with status 201 (Created)

#### Get Order by ID

- **URL**: `/api/v1/orders/{orderId}/users/{userId}`
- **Method**: `GET`
- **Response**: The stop order details or 404 if not found

#### Get Orders by User

- **URL**: `/api/v1/orders/users/{userId}`
- **Method**: `GET`
- **Query Parameters**:
  - `status` (optional): Filter by order status (ACTIVE, TRIGGERED, CANCELLED, EXECUTED, FAILED)
- **Response**: List of stop orders for the user

#### Cancel an Order

- **URL**: `/api/v1/orders/{orderId}/users/{userId}/cancel`
- **Method**: `PATCH`
- **Response**: The updated stop order with status CANCELLED or appropriate error status

#### Stream Notifications

- **URL**: `/api/v1/orders/users/{userId}/notifications`
- **Method**: `GET`
- **Content Type**: `text/event-stream`
- **Response**: Server-Sent Events stream with notifications for the user

## Business Rules

### Stop Orders

1. **Order Types**:
   - **STOP_LOSS**: Triggers when the price falls to or below the trigger price (to limit losses)
   - **STOP_GAIN**: Triggers when the price rises to or above the trigger price (to secure profits)

2. **Order Statuses**:
   - **ACTIVE**: The order is active and waiting for the trigger price to be reached
   - **TRIGGERED**: The order has been triggered because the price condition was met
   - **CANCELLED**: The order was cancelled by the user
   - **EXECUTED**: The order was successfully executed (after being triggered)
   - **FAILED**: The order failed to execute

3. **Order Validation**:
   - Trigger price must be positive
   - Quantity must be positive

4. **Order Cancellation**:
   - Only ACTIVE orders can be cancelled
   - Attempting to cancel an already CANCELLED order will return the order without error
   - Attempting to cancel an order in any other status will result in an error

### Volatility Detection

1. **Volatility Threshold**: Default is 5.0% price change
2. **Time Window**: Default is 15 minutes
3. **Cooldown Period**: 5 minutes between alerts for the same symbol to prevent alert flooding

## Monitoring

The application exposes various metrics through Spring Boot Actuator and Prometheus:

- **Health Metrics**: Application health status
- **Order Metrics**: Number of orders created, triggered, etc.
- **Volatility Metrics**: Number of volatility alerts generated
- **Performance Metrics**: Response times, throughput, etc.

These metrics can be visualized using Grafana dashboards.

## Development

### Project Structure

- `src/main/java/github/gustavodinniz/controller`: API endpoints
- `src/main/java/github/gustavodinniz/service`: Business logic
- `src/main/java/github/gustavodinniz/model`: Data models
- `src/main/java/github/gustavodinniz/repository`: Data access
- `src/main/java/github/gustavodinniz/config`: Application configuration
- `src/main/java/github/gustavodinniz/enumerated`: Enumerations
- `src/main/java/github/gustavodinniz/exception`: Custom exceptions
- `src/main/java/github/gustavodinniz/aop`: Aspect-Oriented Programming components

### Key Components

1. **PriceTickerSimulatorService**: Generates simulated price ticks for testing
2. **VolatilityDetectorService**: Monitors price changes and generates alerts for significant movements
3. **StopOrderProcessorService**: Processes price ticks and triggers orders when conditions are met
4. **OrderManagementService**: Manages the lifecycle of stop orders
5. **NotificationService**: Sends real-time notifications to users

