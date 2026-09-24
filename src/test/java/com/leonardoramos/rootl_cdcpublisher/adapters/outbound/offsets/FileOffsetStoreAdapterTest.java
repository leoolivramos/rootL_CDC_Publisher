package com.leonardoramos.rootl_cdcpublisher.adapters.outbound.offsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class FileOffsetStoreAdapterTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Deve salvar e recuperar offset por conector")
    void shouldSaveAndLoadOffset() {
        FileOffsetStoreAdapter store = new FileOffsetStoreAdapter(tempDir.toString());

        Optional<String> initial = store.load("postgres-test");
        assertTrue(initial.isEmpty(), "Inicialmente não deve haver offset");

        store.save("postgres-test", "0/192CA90");

        Optional<String> loaded = store.load("postgres-test");
        assertTrue(loaded.isPresent());
        assertEquals("0/192CA90", loaded.get());

        // Atualização de offset
        store.save("postgres-test", "0/192D000");
        assertEquals("0/192D000", store.load("postgres-test").get());
    }
}
