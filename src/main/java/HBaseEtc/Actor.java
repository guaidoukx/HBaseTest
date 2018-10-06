package HBaseEtc;




public class Actor extends Thread{
    public void run(){
        for(int count=0; count<10;count++)
            System.out.println(getName() + (count));

    }

    public static void main(String[] args){
        Thread actor = new Actor();
        actor.setName("Mr.Thread");
        actor.start();
        Thread actress = new Thread(new Actress(), "Ms.Runnable");
        actress.start();
        String qq = "1490658363=475251249436=cny= = = = = =c21430314907=824792641273=cny= = = = = =c201= = =x点部=gkb4r=中国=山东省=东营市=广饶县=肯德基=tui hui jian= =山东省东营市广饶县= = = = =0364098= = = = = =中国=江苏省=镇江市=润州区= = = =江苏省镇江市润州区= = = = =18694581523= = = = = = = = = = =";
        System.out.println(qq.split("=").length);


    }
}

class Actress implements Runnable{
    @Override
    public void run() {
        for(int count=0; count<10;count++)
            System.out.println(Thread.currentThread().getName() + (count));
    }
}