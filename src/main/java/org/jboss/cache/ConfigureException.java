package org.jboss.cache;

public class ConfigureException extends Exception {
    private static final long serialVersionUID = -1937579864964670681L;

   public ConfigureException(String str) {
      super(str);
   }

   public ConfigureException(String message, Throwable cause) {
      super(message, cause);
   }
}

