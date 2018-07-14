package HBaseEtc;




public class Actor extends Thread{
    public void run(){
        for(int count=0; count<10000000;count++)
            System.out.println(getName() + (count));
    }

    public static void main(String[] args){
        Thread actor = new Actor();
        actor.setName("Mr.Thread");
        actor.start();
        Thread actress = new Thread(new Actress(), "Ms.Runnable");
        actress.start();

    }
}

class Actress implements Runnable{
    @Override
    public void run() {
        for(int count=0; count<100000000;count++)
            System.out.println(Thread.currentThread().getName() + (count));
    }
}