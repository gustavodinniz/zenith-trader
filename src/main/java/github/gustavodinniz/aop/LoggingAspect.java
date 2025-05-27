package github.gustavodinniz.aop;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;

@Slf4j
@Aspect
@Component
public class LoggingAspect {

    @Pointcut("within(github.gustavodinniz.service..*) && execution(public * *(..))")
    public void serviceMethods() {
    }

    @Pointcut("within(github.gustavodinniz.controller..*) && execution(public * *(..))")
    public void controllerMethods() {
    }

    @Before("serviceMethods() || controllerMethods()")
    public void logMethodEntry(JoinPoint joinPoint) {
        String className = joinPoint.getSignature().getDeclaringTypeName();
        String methodName = joinPoint.getSignature().getName();
        Object[] args = joinPoint.getArgs();
        log.info("ENTERING: {}.{}() with arguments: {}", className, methodName, Arrays.toString(args));
    }

    @AfterThrowing(pointcut = "serviceMethods() || controllerMethods()", throwing = "exception")
    public void logMethodException(JoinPoint joinPoint, Throwable exception) {
        String className = joinPoint.getSignature().getDeclaringTypeName();
        String methodName = joinPoint.getSignature().getName();
        log.error("EXCEPTION em {}.{}(): {}", className, methodName, exception.getMessage(), exception);
    }

    @Around("serviceMethods() || controllerMethods()")
    public Object logAroundReactiveMethods(ProceedingJoinPoint joinPoint) throws Throwable {
        long startTime = System.currentTimeMillis();
        String className = joinPoint.getSignature().getDeclaringTypeName();
        String methodName = joinPoint.getSignature().getName();
        Object[] args = joinPoint.getArgs();
        log.info("AROUND - ENTERING: {}.{}() with arguments: {}", className, methodName, Arrays.toString(args));

        Object result = joinPoint.proceed();

        if (result instanceof Mono<?> monoResult) {
            return monoResult
                    .doOnSuccess(value -> {
                        long duration = System.currentTimeMillis() - startTime;
                        log.info("AROUND - EXITING (Mono Success): {}.{}() | Duração: {}ms | Resultado: {}",
                                className, methodName, duration, value);
                    })
                    .doOnError(error -> {
                        long duration = System.currentTimeMillis() - startTime;
                        log.error("AROUND - EXCEPTION (Mono Error): {}.{}() | Duração: {}ms | Erro: {}",
                                className, methodName, duration, error.getMessage(), error);
                    })
                    .doFinally(signalType -> {
                        if (signalType == reactor.core.publisher.SignalType.CANCEL) {
                            long duration = System.currentTimeMillis() - startTime;
                            log.warn("AROUND - CANCELLED (Mono): {}.{}() | Duração: {}ms", className, methodName, duration);
                        }
                    });
        } else if (result instanceof Flux<?> fluxResult) {
            return fluxResult
                    .doOnSubscribe(subscription ->
                            log.debug("AROUND - SUBSCRIBED (Flux): {}.{}()", className, methodName)
                    )
                    .doOnComplete(() -> {
                        long duration = System.currentTimeMillis() - startTime;
                        log.info("AROUND - COMPLETED (Flux): {}.{}() | Duração: {}ms",
                                className, methodName, duration);
                    })
                    .doOnError(error -> {
                        long duration = System.currentTimeMillis() - startTime;
                        log.error("AROUND - EXCEPTION (Flux Error): {}.{}() | Duração: {}ms | Erro: {}",
                                className, methodName, duration, error.getMessage(), error);
                    })
                    .doFinally(signalType -> {
                        if (signalType == reactor.core.publisher.SignalType.CANCEL) {
                            long duration = System.currentTimeMillis() - startTime;
                            log.warn("AROUND - CANCELLED (Flux): {}.{}() | Duração: {}ms", className, methodName, duration);
                        }
                    });
        } else {
            long duration = System.currentTimeMillis() - startTime;
            log.info("AROUND - EXITING (Sync): {}.{}() | Duração: {}ms | Resultado: {}",
                    className, methodName, duration, result);
            return result;
        }
    }

}
