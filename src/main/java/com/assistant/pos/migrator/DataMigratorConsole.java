package com.assistant.pos.migrator;

public class DataMigratorConsole {
    public static void main(String[] args) {
        String from = "/home/giovanni/Downloads/pos_db.sqlite";
        String to = "/home/giovanni/Sources/Js/PoS2/pos.db.sqlite";

        Migrator migrator = new Migrator();
        migrator.migrate(from, to);
    }
}
