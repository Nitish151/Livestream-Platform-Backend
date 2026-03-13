import * as fs from 'fs';
import * as path from 'path';

enum LogLevel {
    DEBUG = 'DEBUG',
    INFO = 'INFO',
    WARN = 'WARN',
    ERROR = 'ERROR',
}

interface LogEntry {
    timestamp: string;
    level: LogLevel;
    message: string;
    metadata?: Record<string, unknown>;
}

class Logger {
    private logDir: string;
    private minLevel: LogLevel = LogLevel.INFO;

    constructor(logDir: string = './logs') {
        this.logDir = logDir;
        this.ensureLogDir();
    }

    private ensureLogDir(): void {
        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
    }

    private formatEntry(entry: LogEntry): string {
        const meta = entry.metadata ? ` ${JSON.stringify(entry.metadata)}` : '';
        return `[${entry.timestamp}] [${entry.level}] ${entry.message}${meta}`;
    }

    private writeToFile(entry: LogEntry): void {
        const fileName = `${entry.level.toLowerCase()}-${new Date().toISOString().split('T')[0]}.log`;
        const filePath = path.join(this.logDir, fileName);
        fs.appendFileSync(filePath, this.formatEntry(entry) + '\n');
    }

    private log(level: LogLevel, message: string, metadata?: Record<string, unknown>): void {
        const entry: LogEntry = {
            timestamp: new Date().toISOString(),
            level,
            message,
            metadata,
        };

        console.log(this.formatEntry(entry));
        this.writeToFile(entry);
    }

    debug(message: string, metadata?: Record<string, unknown>): void {
        this.log(LogLevel.DEBUG, message, metadata);
    }

    info(message: string, metadata?: Record<string, unknown>): void {
        this.log(LogLevel.INFO, message, metadata);
    }

    warn(message: string, metadata?: Record<string, unknown>): void {
        this.log(LogLevel.WARN, message, metadata);
    }

    error(message: string, metadata?: Record<string, unknown>): void {
        this.log(LogLevel.ERROR, message, metadata);
    }

    setMinLevel(level: LogLevel): void {
        this.minLevel = level;
    }
}

export default new Logger();