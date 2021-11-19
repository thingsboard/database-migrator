package org.thingsboard.client.tools.migrator.exception;


public class EntityMissingException extends RuntimeException {
    public EntityMissingException(String msg) {
        super(msg);
    }
}
