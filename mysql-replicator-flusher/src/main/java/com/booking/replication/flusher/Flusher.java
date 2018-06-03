package com.booking.replication.flusher;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface Flusher {
    enum Type {
        NATIVE {
            @Override
            public Flusher newInstance(Map<String, String> configuration) {
                throw new UnsupportedOperationException("Not yet implemented");
            }
        },
        EXTERNAL {
            @Override
            public Flusher newInstance(Map<String, String> configuration) {
                return new ExternalFlusher(configuration);
            }
        };

        public abstract Flusher newInstance(Map<String, String> configuration);
    }

    interface Configuration {
        String TYPE = "flusher.type";
    }

    void flush(Collection<String> tables) throws IOException;

    @SuppressWarnings("unchecked")
    static Flusher build(Map<String, String> configuration) {
        return Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.EXTERNAL.name())
        ).newInstance(configuration);
    }
}
