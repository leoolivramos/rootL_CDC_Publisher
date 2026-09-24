package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.oracle;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OracleSqlParserTest {

    @Test
    @DisplayName("Deve extrair corretamente o Full State (before e after) em operação de UPDATE descrita no artigo")
    void shouldExtractFullStateFromUpdate() {
        // Exemplo exato da Listagem 5 do artigo
        String redoSql = "UPDATE \"RH\".\"FUNCIONARIOS\" " +
                "SET \"CARGO\" = 'Analista' " +
                "WHERE \"ID\" = '158' " +
                "  AND \"NOME\" = 'Leonardo Ramos' " +
                "  AND \"CARGO\" = 'Estagiario' " +
                "  AND \"DEPARTAMENTO\" = 'Inteligencia'";

        Map<String, Object>[] states = OracleSqlParser.parseUpdateFullState(redoSql);
        Map<String, Object> before = states[0];
        Map<String, Object> after = states[1];

        assertNotNull(before, "Before image não deve ser nula");
        assertNotNull(after, "After image não deve ser nula");

        // Verifica estado anterior (Before)
        assertEquals("158", before.get("ID"));
        assertEquals("Leonardo Ramos", before.get("NOME"));
        assertEquals("Estagiario", before.get("CARGO"));
        assertEquals("Inteligencia", before.get("DEPARTAMENTO"));

        // Verifica estado posterior (After) - CARGO atualizado, demais colunas preservadas
        assertEquals("158", after.get("ID"));
        assertEquals("Leonardo Ramos", after.get("NOME"));
        assertEquals("Analista", after.get("CARGO"));
        assertEquals("Inteligencia", after.get("DEPARTAMENTO"));
    }

    @Test
    @DisplayName("Deve tratar cláusula WHERE com IS NULL conforme Listagem 6 do artigo")
    void shouldHandleIsNullInWhereClause() {
        String redoSql = "UPDATE \"RH\".\"FUNCIONARIOS\" " +
                "SET \"OBSERVACAO\" = 'Primeira Nota' " +
                "WHERE \"ID\" = '158' " +
                "  AND \"OBSERVACAO\" IS NULL";

        Map<String, Object>[] states = OracleSqlParser.parseUpdateFullState(redoSql);
        Map<String, Object> before = states[0];
        Map<String, Object> after = states[1];

        assertEquals("158", before.get("ID"));
        assertNull(before.get("OBSERVACAO"), "Coluna IS NULL deve ter valor nulo no before map");

        assertEquals("158", after.get("ID"));
        assertEquals("Primeira Nota", after.get("OBSERVACAO"));
    }

    @Test
    @DisplayName("Deve analisar instrução INSERT do LogMiner")
    void shouldParseInsert() {
        String insertSql = "INSERT INTO \"RH\".\"FUNCIONARIOS\" (\"ID\", \"NOME\", \"CARGO\") " +
                "VALUES ('100', 'Carlos Silva', 'Engenheiro')";

        Map<String, Object> data = OracleSqlParser.parseInsert(insertSql);
        assertEquals("100", data.get("ID"));
        assertEquals("Carlos Silva", data.get("NOME"));
        assertEquals("Engenheiro", data.get("CARGO"));
    }

    @Test
    @DisplayName("Deve analisar instrução DELETE do LogMiner extraindo Before Image")
    void shouldParseDelete() {
        String deleteSql = "DELETE FROM \"RH\".\"FUNCIONARIOS\" " +
                "WHERE \"ID\" = '100' AND \"NOME\" = 'Carlos Silva'";

        Map<String, Object> before = OracleSqlParser.parseDelete(deleteSql);
        assertEquals("100", before.get("ID"));
        assertEquals("Carlos Silva", before.get("NOME"));
    }
}
