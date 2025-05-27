package github.gustavodinniz.exception;

public class OrderCannotBeCancelledException extends RuntimeException {

    public OrderCannotBeCancelledException(String message) {
        super(message);
    }
}
