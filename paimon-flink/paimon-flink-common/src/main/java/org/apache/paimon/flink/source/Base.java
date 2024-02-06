package org.apache.paimon.flink.source;


// 必须实现的接口
interface FlinkSourceFunction {
    void run();
}

public abstract class Base {

    // using in stream execute
    private int xxx;
    private int yyy;

    //using in batch execute
    private int zzz;
    private int aaa;


    abstract void execute();
    public void streamExecute(){
        //do stream logic

        //and the invoke common logic execute()
        execute();
    }

    public void batchExecute(){
        //do batch logic

        //and the invoke common loigc execute()

        execute();
    }
    abstract void shouldExecute();
}

abstract class Base2 extends Base {

    public void execute(){
        //the invoke common logic1
    }
}

abstract class Base3 extends Base {

    public void execute(){
        //the invoke common logic2
    }
}


class StreamExecutor2 extends Base2 implements FlinkSourceFunction {

    @Override
    public void run(){
        streamExecute();
    }

    @Override
    void shouldExecute() {
        //do some check
    }
}

class StreamExecutor3 extends Base3 implements FlinkSourceFunction {

    @Override
    public void run(){
        streamExecute();
    }

    @Override
    void shouldExecute() {
        //do some check
    }
}

class BatchExecutor2 extends Base2 implements FlinkSourceFunction {
    @Override
    public void run(){
        batchExecute();
    }
    @Override
    void shouldExecute() {
        //do some check
    }
}

class BatchExecutor3 extends Base3 implements FlinkSourceFunction {
    @Override
    public void run(){
        batchExecute();
    }
    @Override
    void shouldExecute() {
        //do some check
    }
}


