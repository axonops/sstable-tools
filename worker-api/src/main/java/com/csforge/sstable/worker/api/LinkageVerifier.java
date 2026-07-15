package com.csforge.sstable.worker.api;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/** Reflection checks for the Cassandra internals required by a release adapter. */
public final class LinkageVerifier {
    private LinkageVerifier() {
    }

    public static void requirePublicMethod(Class<?> owner,
                                           String name,
                                           Class<?> returnType,
                                           Class<?>... parameterTypes) {
        Method method;
        try {
            method = owner.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            throw missing(owner, "method", signature(name, parameterTypes), e);
        }
        if (!Modifier.isPublic(method.getModifiers()) || method.getReturnType() != returnType) {
            throw missing(owner, "public method", signature(name, parameterTypes), null);
        }
    }

    public static void requirePublicStaticMethod(Class<?> owner,
                                                 String name,
                                                 Class<?> returnType,
                                                 Class<?>... parameterTypes) {
        requirePublicMethod(owner, name, returnType, parameterTypes);
        try {
            Method method = owner.getMethod(name, parameterTypes);
            if (!Modifier.isStatic(method.getModifiers())) {
                throw missing(owner, "public static method", signature(name, parameterTypes), null);
            }
        } catch (NoSuchMethodException e) {
            throw missing(owner, "public static method", signature(name, parameterTypes), e);
        }
    }

    public static void requirePublicStaticField(Class<?> owner,
                                                String name,
                                                Class<?> fieldType) {
        Field field;
        try {
            field = owner.getField(name);
        } catch (NoSuchFieldException e) {
            throw missing(owner, "field", name, e);
        }
        int modifiers = field.getModifiers();
        if (!Modifier.isPublic(modifiers) || !Modifier.isStatic(modifiers)
                || field.getType() != fieldType) {
            throw missing(owner, "public static field", name, null);
        }
    }

    public static void requireAssignable(Class<?> contract, Class<?> implementation) {
        if (!contract.isAssignableFrom(implementation)) {
            throw new IllegalStateException(implementation.getName()
                    + " does not implement required contract " + contract.getName());
        }
    }

    private static IllegalStateException missing(Class<?> owner,
                                                 String kind,
                                                 String member,
                                                 Throwable cause) {
        String message = "Required Cassandra " + kind + " is unavailable: "
                + owner.getName() + "." + member;
        return cause == null ? new IllegalStateException(message)
                : new IllegalStateException(message, cause);
    }

    private static String signature(String name, Class<?>[] parameterTypes) {
        StringBuilder result = new StringBuilder(name).append('(');
        for (int index = 0; index < parameterTypes.length; index++) {
            if (index > 0) {
                result.append(',');
            }
            result.append(parameterTypes[index].getName());
        }
        return result.append(')').toString();
    }
}
