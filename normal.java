// normal.java
import java.util.concurrent.*;
import java.nio.*;
import java.nio.file.*;
import java.nio.channels.*;
import java.io.*;

public class normal {
    // 用户名称字符长度
    public static final int VLEN = 16;
    public static final String relateFileName = "dataset/relations.txt";
    public static final String accFileName = "dataset/accounts.txt";
    public static final String resultFileName = "result.txt";

    // 主程序主要进行线程声明和管理
    public static void main (String[] args) throws FileNotFoundException, InterruptedException, ExecutionException, IOException {
        // 建立线程池
        long startTime = System.currentTimeMillis();
        File relateFile = new File(relateFileName);
        int relateDataLen = (int)relateFile.length();
        
        ExecutorService service = Executors.newCachedThreadPool();
        // 预估关系数据量
        int relateRows = relateDataLen/(VLEN+8);
        int[] depPairList = new int[relateRows];
        int batN = 5;
        double[] batRatio = {0, 0.1037, 0.2420, 0.4264, 0.6722, 1.0};
        // int batN = 6;
        // double[] batRatio = {0.0, 0.0722, 0.1684, 0.2967, 0.4678, 0.6959, 1.0};
        int[] batStartLoc = new int[batN];
        int[] batRowStart = new int[batN];
        int[] batLastRow = new int[batN];
        for (int i = 0; i < batN; i++) {
            batStartLoc[i] = (int)(relateDataLen*batRatio[i]);
            batRowStart[i] = (int)(relateRows*batRatio[i]/2);
        }
        
        service.submit(new TransInputTASK(depPairList, 0, batStartLoc[1]+64, 0, batLastRow, 0, false));
        // 第二批以后需要进行换行符识别，来确认转化开端
        for (int itask = 1; itask < batN; itask++) {
            if (itask < (batN-1)) {
                service.submit(new TransInputTASK(depPairList, batStartLoc[itask], batStartLoc[itask+1]-batStartLoc[itask]+64, batRowStart[itask], batLastRow, itask, true)); 
            } else {
                service.submit(new TransInputTASK(depPairList, batStartLoc[itask], relateDataLen-batStartLoc[itask], batRowStart[itask], batLastRow, itask, true)); 
            }
        }
        
        // 启动关联表计算线程
        Future<int[]> resGenList = service.submit(new GenListTASK(depPairList, batLastRow, batN, batRowStart));
        Future<int[]> resHandleAcc = service.submit(new HandleDataTASK());
        // 启动账号数据线程
        Future<byte[]> resAccIn = service.submit(new ReadAccFileTASK());

        // 操作输出文件初始化
        File fout = new File(normal.resultFileName);
        if (fout.exists()) {
            fout.delete();
        }
        fout.createNewFile();
                
        Path resPath = Paths.get(normal.resultFileName);
        FileChannel fc = FileChannel.open(resPath, StandardOpenOption.READ, StandardOpenOption.WRITE);

        File accFile = new File(accFileName); 
        int accDataLen = (int)accFile.length();
        int numOfAcc = accDataLen/(VLEN+4);
        numOfAcc = accDataLen/(normal.VLEN+2+(int)(Math.log10(numOfAcc)));
        // 计录结果文件每一行的安排位置
        int[] locOfArrange = new int[numOfAcc];
        byte[] outputCon = new byte[relateDataLen];
        
        // 行位置标识数据唤回，并从中取出用户数量
        int[] rowStartLoc = resHandleAcc.get();
        numOfAcc = rowStartLoc[0];
        rowStartLoc[0] = 0;
        // 取回字节化的帐号数据
        byte[] accContent = resAccIn.get();
        // 分片读入数据并进行依赖关系计算
        int[] dependList = resGenList.get();

        // 分三段生成 2/10, 3/10, 5/10
        int x0 = 0;
        Future<Integer> resTxtGenONE = service.submit(new OutputGenTASK(accContent, outputCon, dependList, rowStartLoc, locOfArrange, 0, numOfAcc/10*3, x0, numOfAcc));
        int x1 = (int)(relateDataLen*0.333);
        Future<Integer> resTxtGenTWO = service.submit(new OutputGenTASK(accContent, outputCon, dependList, rowStartLoc, locOfArrange, numOfAcc/10*3, numOfAcc/11*7, x1, numOfAcc));
        int x2 = (int)(relateDataLen*0.666);
        Future<Integer> resTxtGenTHREE = service.submit(new OutputGenTASK(accContent, outputCon, dependList, rowStartLoc, locOfArrange, numOfAcc/11*7, numOfAcc, x2, numOfAcc));
        service.shutdown();

        int y0 = resTxtGenONE.get();
        int y1 = resTxtGenTWO.get();
        int y2 = resTxtGenTHREE.get();
        // 将前面结果写入文件
        MappedByteBuffer mbuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, y0-x0+y1-x1+y2-x2);
        mbuf.put(outputCon, x0, y0-x0);
        mbuf.put(outputCon, x1, y1-x1);
        mbuf.put(outputCon, x2, y2-x2);
        fc.close();

        System.out.println("ALL DONE. Elapse " + (System.currentTimeMillis()-startTime) + "ms");
    }
}

// 读入文件并转化
class TransInputTASK implements Runnable{
    private int[] depPairList;
    private int offLoc;
    private int dataLen;
    private int startRow;
    private int[] batLastRow;
    private int batchID;
    private boolean checkHead;

    public TransInputTASK(int[] depvec, int floc, int len, int strow, int[] btends, int ibt, boolean chk) {
        this.depPairList = depvec;
        this.offLoc = floc;
        this.dataLen = len;
        this.startRow = strow;
        this.batLastRow = btends;
        this.batchID = ibt;
        this.checkHead = chk;
    }

    @Override
    public void run(){
        byte[] bytesCon = new byte[dataLen];
        try {
            Path relatePath = Paths.get(normal.relateFileName);
            FileChannel fc = FileChannel.open(relatePath, StandardOpenOption.READ);
            MappedByteBuffer mbuf = fc.map(FileChannel.MapMode.READ_ONLY, offLoc, dataLen);
            mbuf.get(bytesCon);
            fc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        int i = 0;
        if (checkHead) {
            // 这个循环没有内容
            while (bytesCon[++i] != '\n') {}

            i++;
        }

        int transInt = 0;        
        int p = startRow*2;
        int abc = 0;
        int dsn = (int)('\n' - '0');
        while (i < dataLen) {
            abc = (int)(bytesCon[i] - '0');
            if (abc >= 0) {
                transInt *= 10;
                transInt += abc;
            } else if (abc != dsn) {
                depPairList[p++] = transInt;
                i += (normal.VLEN + 1);
                transInt = 0;
            } 
            i++;
        }

        batLastRow[batchID] = p/2-1;
    }
}

// 读文件线程任务
class ReadAccFileTASK implements Callable <byte[]> {

    @Override
    public byte[] call() throws Exception, IOException, FileNotFoundException{
        
        File accFile = new File(normal.accFileName); 
        int accDataLen = (int)accFile.length();
        byte[] fileContent = new byte[accDataLen];

        Path relatePath = Paths.get(normal.accFileName);
        FileChannel fc = FileChannel.open(relatePath, StandardOpenOption.READ);

        MappedByteBuffer mbuf = fc.map(FileChannel.MapMode.READ_ONLY, 0, accDataLen);
        mbuf.get(fileContent);
        fc.close();
        
        return fileContent;
    }
}

// 行首位置提取线程任务
class HandleDataTASK implements Callable <int[]> {
    @Override
    public int[] call() throws Exception {
        File accFile = new File(normal.accFileName); 
        int accDataLen = (int)accFile.length();
        int numOfAcc = accDataLen/(normal.VLEN+4);
        numOfAcc = accDataLen/(normal.VLEN+2+(int)(Math.log10(numOfAcc)));

        int[] rowStartLoc = new int[numOfAcc];
        int intLen = 1;
        int intBound = 10;
        int nowLoc = 0;
        int i = 0;
        while (nowLoc < accDataLen) {
            i++;
            nowLoc += (normal.VLEN + intLen + 3);
            rowStartLoc[i] = nowLoc;
            if (i >= intBound) {
                intBound *= 10;
                intLen++;
            }
        }
        rowStartLoc[0] = i;

        return rowStartLoc;
    }  
}

// 依赖关系分析任务
class GenListTASK implements Callable <int[]> {
    private int[] depPairList;
    private int[] batLastRow;
    private int batN;
    private int[] batRowStart;
    
    public GenListTASK(int[] depcom, int[] btlrow, int btn, int[] brs) {
        this.depPairList = depcom;
        this.batLastRow = btlrow;
        this.batN = btn;
        this.batRowStart = brs;
    }
    
    @Override
    public int[] call() throws Exception {
        File accFile = new File(normal.accFileName); 
        int accDataLen = (int)accFile.length();
        int numOfAcc = accDataLen/(normal.VLEN+4);
        // 为了让行数估计更精确
        numOfAcc = accDataLen/(normal.VLEN+2+(int)(Math.log10(numOfAcc)));
        int i = 0;
        int[] dependList = new int[numOfAcc];
        for (i = 0; i < numOfAcc; i++) {
            dependList[i] = i; 
        }

        int idFront = 0;
        int idBehind = 0;
        int depFront = 0;
        int depBehind = 0;
        for (int itask = 0; itask < batN; itask++) {
            while (batLastRow[itask] == 0) {
                Thread.sleep(1);
            }

            for (i = batRowStart[itask]*2; i <= batLastRow[itask]*2; i++) {
                idFront = depPairList[i++];
                idBehind = depPairList[i];
                depFront = dependList[idFront];
                depBehind = dependList[idBehind];

                if (depFront < depBehind) {
                    if (idBehind != depBehind) { 
                        dependList[depBehind] = depFront; 
                    }
                    dependList[idBehind] = depFront;
                } else if (depFront > depBehind) {
                    if (idFront != depFront) {
                        dependList[depFront] = depBehind; 
                    }
                    dependList[idFront] = depBehind;
                }
            }
        }
        // 按编号进行归一化梳理，同时用集合起始位记录频次数
        for (i = 0; i < numOfAcc; i++) {
            idFront = dependList[i];
            if (idFront != i)  {
                depFront = dependList[idFront];

                if (depFront >= 0) {
                    dependList[i] = depFront;
                    dependList[depFront]--;
                } else {
                    dependList[idFront]--;
                }
            } else {
                dependList[i] = -1;
            }
        }
        
        return dependList;
    }  
}

// 将后面的数据转化为输出字节
class OutputGenTASK implements Callable <Integer> {
    private byte[] accContent;
    private byte[] outBytes;
    private int[] dependList;
    private int[] rowStartLoc;
    private int[] locOfArrange;
    private int startRowId;
    private int stopRowId;
    private int txtStartLoc;
    private int numOfAcc;
    
    public OutputGenTASK(byte[] indata, byte[] outbts, int[] deplist, int[] rstlist, int[] localist, int rowstid, int rownid, int btst, int nrows) {
        this.accContent = indata;
        this.outBytes = outbts;
        this.dependList = deplist;
        this.rowStartLoc = rstlist;
        this.locOfArrange = localist;
        this.startRowId = rowstid;
        this.stopRowId = rownid;
        this.txtStartLoc = btst;
        this.numOfAcc = nrows;
    }

    @Override
    public Integer call() throws Exception{
        int currentRootId = 0;
        int locPut = 0;
        int k = 0;
        int z = txtStartLoc;
        int irspl = 0;
        for (int i = startRowId; i < numOfAcc; i++) {
            irspl = rowStartLoc[i+1]-2;

            if (dependList[i] < 0) {
                if (i < stopRowId) {
                    k = rowStartLoc[i];
                    System.arraycopy(accContent, k, outBytes, z, irspl-k);
                    z += (irspl-k);
                    locOfArrange[i] = z;
                    // 因为在初始位记录统计次数用相反数表示
                    z -= (dependList[i]+1)*(normal.VLEN+1);
                    outBytes[z++] = '\r';
                    outBytes[z++] = '\n';
                }
            } else {
                currentRootId = dependList[i];
                if ((currentRootId >= startRowId) && (currentRootId < stopRowId)) {
                    locPut = locOfArrange[currentRootId];
                    locOfArrange[currentRootId] += (normal.VLEN+1);
                    outBytes[locPut++] = ',';
                    System.arraycopy(accContent, (irspl-normal.VLEN), outBytes, locPut, normal.VLEN);
                }
            }
        }
        return z;
    }
}
