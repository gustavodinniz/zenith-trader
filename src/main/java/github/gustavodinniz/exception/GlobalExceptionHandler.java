package github.gustavodinniz.exception;

import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Instant;

@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(OrderNotFoundException.class)
    public Mono<ProblemDetail> handleOrderNotFoundException(OrderNotFoundException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                HttpStatus.NOT_FOUND, ex.getMessage());

        enrichProblemDetail(problemDetail, "Order Not Found", "/errors/not-found");

        return Mono.just(problemDetail);
    }

    @ExceptionHandler(OrderCannotBeCancelledException.class)
    public Mono<ProblemDetail> handleOrderCannotBeCancelledException(OrderCannotBeCancelledException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                HttpStatus.CONFLICT, ex.getMessage());

        enrichProblemDetail(problemDetail, "Order Cannot Be Cancelled", "/errors/conflict");

        return Mono.just(problemDetail);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public Mono<ProblemDetail> handleIllegalArgumentException(IllegalArgumentException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, ex.getMessage());

        enrichProblemDetail(problemDetail, "Invalid Request Parameters", "/errors/bad-request");

        return Mono.just(problemDetail);
    }

    @ExceptionHandler(OptimisticLockingFailureException.class)
    public Mono<ProblemDetail> handleOptimisticLockingFailureException(OptimisticLockingFailureException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                HttpStatus.CONFLICT, "The resource was modified by another request. Please try again.");

        enrichProblemDetail(problemDetail, "Concurrent Modification", "/errors/conflict");

        return Mono.just(problemDetail);
    }

    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<ProblemDetail> handleWebExchangeBindException(WebExchangeBindException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "Validation error");

        enrichProblemDetail(problemDetail, "Validation Failed", "/errors/validation");

        // Add validation errors as properties
        ex.getBindingResult().getFieldErrors().forEach(fieldError ->
                problemDetail.setProperty(fieldError.getField(), fieldError.getDefaultMessage())
        );

        return Mono.just(problemDetail);
    }

    @ExceptionHandler(ServerWebInputException.class)
    public Mono<ProblemDetail> handleServerWebInputException(ServerWebInputException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, ex.getReason());

        enrichProblemDetail(problemDetail, "Invalid Input", "/errors/bad-request");

        return Mono.just(problemDetail);
    }

    @ExceptionHandler(ResponseStatusException.class)
    public Mono<ProblemDetail> handleResponseStatusException(ResponseStatusException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                ex.getStatusCode(), ex.getReason());

        enrichProblemDetail(problemDetail, "Request Error", "/errors/request-error");

        return Mono.just(problemDetail);
    }

    @ExceptionHandler(Exception.class)
    public Mono<ProblemDetail> handleGenericException(Exception ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");

        enrichProblemDetail(problemDetail, "Internal Server Error", "/errors/server-error");

        return Mono.just(problemDetail);
    }

    private void enrichProblemDetail(ProblemDetail problemDetail, String title, String type) {
        problemDetail.setTitle(title);
        problemDetail.setType(URI.create(type));
        problemDetail.setProperty("timestamp", Instant.now());
    }
}
