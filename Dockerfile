# Fase 1: Build da aplicação com Maven usando Java 21
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /app
COPY pom.xml .
# Opcional: Se você tiver um settings.xml ou um wrapper do Maven, copie-os também.
# COPY .mvn/ .mvn
# COPY mvnw .
# COPY pom.xml .
# RUN ./mvnw dependency:resolve # Para baixar dependências antes de copiar o código fonte, pode acelerar builds subsequentes se o pom.xml não mudar
RUN mvn dependency:go-offline -B # Alternativa para baixar dependências

COPY src ./src
RUN mvn clean package -DskipTests

# Fase 2: Criação da imagem final com o JRE Java 21
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
# Copia o JAR construído da fase anterior
COPY --from=builder /app/target/*.jar app.jar

# Expõe a porta que a aplicação usa (definida no application.yml)
EXPOSE 8080

# Comando para executar a aplicação
ENTRYPOINT ["java", "-jar", "app.jar"]