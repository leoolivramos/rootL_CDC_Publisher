package com.leonardoramos.rootl_cdcpublisher.domain.services;

import com.leonardoramos.rootl_cdcpublisher.domain.model.ChangeEvent;
import com.leonardoramos.rootl_cdcpublisher.domain.model.OperationType;
import com.leonardoramos.rootl_cdcpublisher.domain.model.SourceMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TransactionBufferTest {

    private TransactionBuffer buffer;

    @BeforeEach
    void setUp() {
        buffer = new TransactionBuffer();
    }

    private ChangeEvent createDummyEvent(String txId, OperationType op) {
        SourceMetadata meta = SourceMetadata.postgres("test-conn", "test-db", "public", "clientes", txId, "0/1000");
        return new ChangeEvent(UUID.randomUUID(), op, Instant.now(), meta, Map.of("id", "1"), Map.of("id", "1", "nome", "Teste"));
    }

    @Test
    @DisplayName("Deve reter eventos em memória e liberá-los somente no COMMIT")
    void shouldBufferEventsUntilCommit() {
        String txId = "tx-1001";
        ChangeEvent event1 = createDummyEvent(txId, OperationType.INSERT);
        ChangeEvent event2 = createDummyEvent(txId, OperationType.UPDATE);

        buffer.addEvent(txId, event1);
        buffer.addEvent(txId, event2);

        assertEquals(2.0, buffer.getTotalEventsCount());

        List<ChangeEvent> committed = buffer.commit(txId);
        assertEquals(2, committed.size());
        assertEquals(event1, committed.get(0));
        assertEquals(event2, committed.get(1));

        // Buffer deve estar vazio após o commit
        assertEquals(0.0, buffer.getTotalEventsCount());
        assertTrue(buffer.commit(txId).isEmpty());
    }

    @Test
    @DisplayName("Deve descartar eventos de transações abortadas (ROLLBACK)")
    void shouldDiscardEventsOnRollback() {
        String txId = "tx-rollback";
        ChangeEvent event = createDummyEvent(txId, OperationType.INSERT);

        buffer.addEvent(txId, event);
        assertEquals(1.0, buffer.getTotalEventsCount());

        buffer.rollback(txId);
        assertEquals(0.0, buffer.getTotalEventsCount());

        List<ChangeEvent> committed = buffer.commit(txId);
        assertTrue(committed.isEmpty(), "Eventos de transação revertida não devem ser liberados");
    }

    @Test
    @DisplayName("Deve respeitar limite máximo de eventos por transação para proteger memória")
    void shouldRespectMaxEventsLimit() {
        String txId = "tx-bulk";
        for (int i = 0; i < 10_050; i++) {
            buffer.addEvent(txId, createDummyEvent(txId, OperationType.INSERT));
        }

        List<ChangeEvent> committed = buffer.commit(txId);
        assertEquals(10_000, committed.size(), "Buffer deve limitar no máximo a 10.000 eventos por transação");
    }
}
