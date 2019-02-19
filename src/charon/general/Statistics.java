package charon.general;

import com.bethecoder.ascii_table.ASCIITable;
import java.util.Date;

public class Statistics {

    private static int lock;
    private static int timeLock;
    private static int renew;
    private static int timeRenew;

    private static int getMeta;

    private static int putMeta;

    private static int delMeta;

    private static int updateMeta;

    private static int getDirMeta;

    private static int readDisk;
    private static long timeReadDisk;
    private static long numBytesReadDisk;

    private static int readCoC;
    private static long timeReadCoC;
    private static long numBytesReadCoC;

    private static int writeDisk;
    private static long timeWriteDisk;
    private static long numBytesWriteDisk;

    private static int writeCoc;
    private static long timeWriteCoC;
    private static long numBytesWriteCoC;

    private static boolean reseted;

    private static long initPoint;
    private static long endPoint;

    private static long timeGetMeta;
    private static long timePutMeta;
    private static long timeDelMeta;
    private static long timeUpdateMeta;
    private static long timeGetDirMeta;

    private static int readSingleCloud;
    private static int timeReadSingleCloud;
    private static int numBytesReadSingleCloud;
    
    private static int writeSingleCloud;
    private static long timeWriteSingleCloud;
    private static long numBytesWriteSingleCloud;

    public static void init() {
//		System.out.println("»»» Init called ...");
        readCoC = writeCoc = getMeta = putMeta = delMeta = updateMeta = getDirMeta = lock = renew = 0;
        timeReadCoC = timeWriteCoC = timeGetMeta = timePutMeta = timeDelMeta = timeUpdateMeta = timeGetDirMeta = 0;

        numBytesReadCoC = numBytesReadDisk = numBytesWriteCoC = numBytesWriteDisk = 0;
        reseted = false;
        initPoint = System.currentTimeMillis();
        endPoint = initPoint;

    }

    public static void getMeta(long time) {
        if (reseted) {
            init();
        }

        getMeta++;
        timeGetMeta += time;

        endPoint = System.currentTimeMillis();
    }

    public static void putMeta(long time) {
        if (reseted) {
            init();
        }

        putMeta++;
        timePutMeta += time;
        endPoint = System.currentTimeMillis();
    }

    public static void delMeta(long time) {
        if (reseted) {
            init();
        }

        delMeta++;
        timeDelMeta += time;
        endPoint = System.currentTimeMillis();
    }

    public static void updateMeta(long time) {
        if (reseted) {
            init();
        }

        updateMeta++;
        timeUpdateMeta += time;
        endPoint = System.currentTimeMillis();
    }

    public static void getDirMeta(long time) {
        if (reseted) {
            init();
        }

        getDirMeta++;
        timeGetDirMeta += time;
        endPoint = System.currentTimeMillis();
    }

    public static void readCoC(long time, int numBytes) {
        if (reseted) {
            init();
        }

        readCoC++;
        timeReadCoC += time;
        numBytesReadCoC += numBytes;
        endPoint = System.currentTimeMillis();
    }

    public static void readSingleCloud(long time, int numBytes) {
        if (reseted) {
            init();
        }

        readSingleCloud++;
        timeReadSingleCloud += time;
        numBytesReadSingleCloud += numBytes;
        endPoint = System.currentTimeMillis();
    }

    public static void writeSingleCloud(long time, int numBytes) {
        if (reseted) {
            init();
        }

        writeSingleCloud++;
        timeWriteSingleCloud += time;
        numBytesWriteSingleCloud += numBytes;
        endPoint = System.currentTimeMillis();
    }

    public static void readPrivateRep(long time, int numBytes) {
        if (reseted) {
            init();
        }

        readCoC++;
        timeReadCoC += time;
        numBytesReadCoC += numBytes;
        endPoint = System.currentTimeMillis();
    }

    public static void readDisk(long time, int numBytes) {
        if (reseted) {
            init();
        }

        readDisk++;
        timeReadDisk += time;
        numBytesReadDisk += numBytes;
        endPoint = System.currentTimeMillis();
    }

    public static void lock(long time) {
        if (reseted) {
            init();
        }

        timeLock += time;

        lock++;
        endPoint = System.currentTimeMillis();
    }

    public static void renew(long time) {
        if (reseted) {
            init();
        }

        timeRenew += time;

        renew++;
        endPoint = System.currentTimeMillis();
    }

    public static void writeCoC(long time, int numBytes) {
        if (reseted) {
            init();
        }

        writeCoc++;
        timeWriteCoC += time;
        numBytesWriteCoC += numBytes;
        endPoint = System.currentTimeMillis();
    }

    public static void writeDisk(long time, int numBytes) {
        if (reseted) {
            init();
        }

        writeDisk++;
        timeWriteDisk += time;
        numBytesWriteDisk += numBytes;
    }

    public static void reset() {
        reseted = true;
    }

    public static String getReport() {

        // start and end
        String res = "======================================================================\n";
        res = res.concat("start:\t " + new Date(initPoint).toString() + "\nend:\t " + new Date(endPoint).toString() + "\n");

        //METADATA
        res = res.concat("\n============================== METADATA ==============================\n");

        //fuse ops
        res = res.concat("\n - FUSE.\n");
        String[] header = new String[]{"", "#Count"};
        String[][] data = new String[7][];
        data[0] = new String[]{"getattr", String.valueOf(opCount[GETATTR])};
        data[1] = new String[]{"getdir", String.valueOf(opCount[GETDIR])};
        data[2] = new String[]{"mkdir", String.valueOf(opCount[MKDIR])};
        data[3] = new String[]{"rmdir", String.valueOf(opCount[RMDIR])};
        data[4] = new String[]{"mknod", String.valueOf(opCount[MKNOD])};
        data[5] = new String[]{"unlink", String.valueOf(opCount[UNLINK])};
        data[6] = new String[]{"truncate", String.valueOf(opCount[TRUNCATE])};

        String[] meta1 = ASCIITable.getInstance().getTable(header, data).split("\n");

        header = new String[]{"", "#Count"};
        data = new String[7][];
        data[0] = new String[]{"rename", String.valueOf(opCount[RENAME])};
        data[1] = new String[]{"utime", String.valueOf(opCount[UTIME])};
        data[2] = new String[]{"chmod", String.valueOf(opCount[CHMOD])};
        data[3] = new String[]{"chown", String.valueOf(opCount[CHOWN])};
        data[4] = new String[]{"symlink", String.valueOf(opCount[SYMLINK])};
        data[5] = new String[]{"link", String.valueOf(opCount[LINK])};
        data[6] = new String[]{"readlink", String.valueOf(opCount[READLINK])};

        String[] meta2 = ASCIITable.getInstance().getTable(header, data).split("\n");

        for (int i = 0; i < meta1.length; i++) {
            res = res.concat(meta1[i] + "   " + meta2[i] + "\n");
        }

        //directory service
        res = res.concat("\n - Directory Service.\n");
        header = new String[]{"", "#Count", "Time (ms)"};
        data = new String[5][];
        data[0] = new String[]{"getMetadata", String.valueOf(getMeta), String.valueOf(timeGetMeta / (getMeta > 0 ? getMeta : 1))};
        data[1] = new String[]{"putMedata", String.valueOf(putMeta), String.valueOf(timePutMeta / (putMeta > 0 ? putMeta : 1))};
        data[2] = new String[]{"deleteMetadata", String.valueOf(delMeta), String.valueOf(timeDelMeta / (delMeta > 0 ? delMeta : 1))};
        data[3] = new String[]{"updateMetadata", String.valueOf(updateMeta), String.valueOf(timeUpdateMeta / (updateMeta > 0 ? updateMeta : 1))};
        data[4] = new String[]{"getDir", String.valueOf(getDirMeta), String.valueOf(timeGetDirMeta / (getDirMeta > 0 ? getDirMeta : 1))};

        res = res.concat(ASCIITable.getInstance().getTable(header, data));

        //lock service
        res = res.concat("\n - Leasing Service.\n");
        header = new String[]{"", "#Count", "Time (ms)"};
        data = new String[2][];
        data[0] = new String[]{"lock", String.valueOf(lock), String.valueOf(timeLock / (lock > 0 ? lock : 1))};
        data[1] = new String[]{"renew", String.valueOf(renew), String.valueOf(timeRenew / (renew > 0 ? renew : 1))};

        res = res.concat(ASCIITable.getInstance().getTable(header, data));

        // DATA
        res = res.concat("\n================================ DATA ================================\n");

        //fuse ops
        res = res.concat("\n - FUSE.\n");
        header = new String[]{"", "#Count"};
        data = new String[6][];
        data[0] = new String[]{"open", String.valueOf(opCount[OPEN])};
        data[1] = new String[]{"write", String.valueOf(opCount[WRITE])};
        data[2] = new String[]{"read", String.valueOf(opCount[READ])};
        data[3] = new String[]{"flush", String.valueOf(opCount[FLUSH])};
        data[4] = new String[]{"fsync", String.valueOf(opCount[FSYNC])};
        data[5] = new String[]{"release", String.valueOf(opCount[RELEASE])};

        res = res.concat(ASCIITable.getInstance().getTable(header, data));

        //storage service
        res = res.concat("\n - Storage Service.\n");
        //CoC
        header = new String[]{"", "#Count", "Time (ms)"};
        data = new String[2][];
        data[0] = new String[]{"read", String.valueOf(readCoC), String.valueOf(timeReadCoC / (readCoC > 0 ? readCoC : 1))};
        data[1] = new String[]{"write", String.valueOf(writeCoc), String.valueOf(timeWriteCoC / (writeCoc > 0 ? writeCoc : 1))};

        String[] CoC = ASCIITable.getInstance().getTable(header, data).split("\n");

        //SingleCloud
        header = new String[]{"", "#Count", "Time (ms)"};
        data = new String[2][];
        data[0] = new String[]{"read", String.valueOf(readSingleCloud), String.valueOf(timeReadSingleCloud / (readSingleCloud > 0 ? readSingleCloud : 1))};
        data[1] = new String[]{"write", String.valueOf(writeCoc), String.valueOf(timeWriteCoC / (writeCoc > 0 ? writeCoc : 1))};

        String[] singleCloud = ASCIITable.getInstance().getTable(header, data).split("\n");

        for (int i = 0; i < CoC.length; i++) {
            res = res.concat(CoC[i] + "   " + singleCloud[i] + "\n");
        }

        return res;

//        res = res.concat("\t--\n");
//        res = res.concat("DepSky:\n");
//        res = res.concat("\tread: " + read + "(" + numBytesRead + ")\t" + timeRead / (read > 0 ? read : 1) + " ms\n");
//        res = res.concat("\twrite: " + write + "(" + numBytesWrite + ")\t" + timeWrite / (write > 0 ? write : 1) + " ms\n");
//
//        res = res.concat("\t--\n");
//        res = res.concat("Disk:\n");
//        res = res.concat("\tread: " + readInDisk + "(" + numBytesReadInDisk + ")\t" + timeReadInDik / (readInDisk > 0 ? readInDisk : 1) + " nanos\n");
//        res = res.concat("\twrite: " + writeInDisk + "(" + numBytesWriteInDisk + ")\t" + timeWriteInDisk / (writeInDisk > 0 ? writeInDisk : 1) + " nanos\n");
//
//        res = res.concat("\t--\n");
//        res = res.concat("Memory:\n");
//        res = res.concat("\tread: " + readInMem + "(" + numBytesReadInMem + ")\t" + timeReadInMem / (readInMem > 0 ? readInMem : 1) + " nanos\n");
//        res = res.concat("\twrite: " + writeInMem + "(" + numBytesWriteInMem + ")\t" + timeWriteInMem / (writeInMem > 0 ? writeInMem : 1) + " nanos\n");
//
//        res = res.concat("\t--\n");
//        res = res.concat("Lock Service:\n");
//        res = res.concat("\ttryAquire: " + open + "\n");
//        res = res.concat("\trelease: " + close + "\n");
//        return res;
    }

    private static String[] ops = new String[]{"getattr", "getdir", "mkdir", "rmdir", "mknod", "unlink", "read", "write", "open", "flush", "fsync", "release",
        "truncate", "rename", "utime", "chmod", "chown", "symlink", "link", "readlink", "statfs"};

    private static long[] opCount = new long[ops.length];

    public static void execOp(int op) {
        opCount[op]++;
    }

    public static int GETATTR = 0;
    public static int GETDIR = 1;
    public static int MKDIR = 2;
    public static int RMDIR = 3;
    public static int MKNOD = 4;
    public static int UNLINK = 5;
    public static int READ = 6;
    public static int WRITE = 7;
    public static int OPEN = 8;
    public static int FLUSH = 9;
    public static int FSYNC = 10;
    public static int RELEASE = 11;
    public static int TRUNCATE = 12;
    public static int RENAME = 13;
    public static int UTIME = 14;
    public static int CHMOD = 15;
    public static int CHOWN = 16;
    public static int SYMLINK = 17;
    public static int LINK = 18;
    public static int READLINK = 19;
    public static int STATFS = 20;

}
