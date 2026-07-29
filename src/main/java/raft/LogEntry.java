package raft;

public class LogEntry {
    private final int term;
    private final String command; // Expected format: "PUT key=value" or "DEL key"

    public LogEntry(int term, String command) {
        this.term = term;
        this.command = command;
    }

    public int getTerm() { return term; }
    public String getCommand() { return command; }

    @Override
    public String toString() {
        return "[" + command + " (Term " + term + ")]";
    }
}
