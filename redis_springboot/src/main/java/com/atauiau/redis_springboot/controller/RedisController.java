package com.atauiau.redis_springboot.controller;

import io.netty.util.internal.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.script.ScriptExecutor;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/redisTest")
@SuppressWarnings("all")
public class RedisController {

    @Autowired
    private RedisTemplate redisTemplate;

    @GetMapping("/getValue")
    public String testRedis(){
        //设置值到Redis中
        redisTemplate.opsForValue().set("name","lucry");
        //从redis中取值
        String name = (String)redisTemplate.opsForValue().get("name");
        return name;
    }

    @GetMapping ("/spike")
    public void  spikeHandler(HttpServletResponse response) throws IOException {
        //随机生成用户id
        //boolean b = spikeMethod(this.getCode(),"0101");
        boolean b = spikeByScript(this.getCode(), "0101");
        response.getWriter().println(b);
    }

    @GetMapping("testLock")
    public void testLock(){
        //生成uuid
        String uuid = UUID.randomUUID().toString();
        //获取锁
        Boolean lock = redisTemplate.opsForValue().setIfAbsent("lock",uuid,3, TimeUnit.SECONDS);
        if(lock){
            //获取到锁了
            Object value = redisTemplate.opsForValue().get("num");
            //判断num为空 return
            if(StringUtils.isEmpty(value)){
                redisTemplate.delete("lock");
                return;
            }
            //有值就转成int
            int num = Integer.parseInt(value.toString());
            //把值自增放入redis
            redisTemplate.opsForValue().set("num",++num);
            //获取对应的锁
            Object lockUUID = redisTemplate.opsForValue().get("lock");
            //判断当前redis的锁是否为自己的锁
            if(uuid.equals(lockUUID)){
                //释放自己的锁，防止误删
                redisTemplate.delete("lock");
            }
            return;
        }

        //没有获取到锁就睡眠0.1秒再获取
        try {
            Thread.sleep(100);
            testLock();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    @GetMapping("testLockLua")
    public void testLockLua() {
        //1 声明一个uuid ,将做为一个value 放入我们的key所对应的值中
        String uuid = UUID.randomUUID().toString();
        //2 定义一个锁：lua 脚本可以使用同一把锁，来实现删除！
        String skuId = "25"; // 访问skuId 为25号的商品 100008348542
        String locKey = "lock:" + skuId; // 锁住的是每个商品的数据

        // 3 获取锁
        Boolean lock = redisTemplate.opsForValue().setIfAbsent(locKey, uuid, 3, TimeUnit.SECONDS);

        // 第一种： lock 与过期时间中间不写任何的代码。
        // redisTemplate.expire("lock",10, TimeUnit.SECONDS);//设置过期时间
        // 如果true
        if (lock) {
            // 执行的业务逻辑开始
            // 获取缓存中的num 数据
            Object value = redisTemplate.opsForValue().get("num");
            // 如果是空直接返回
            if (StringUtils.isEmpty(value)) {
                return;
            }
            // 不是空 如果说在这出现了异常！ 那么delete 就删除失败！ 也就是说锁永远存在！
            int num = Integer.parseInt(value + "");
            // 使num 每次+1 放入缓存
            redisTemplate.opsForValue().set("num", String.valueOf(++num));
            /*使用lua脚本来锁*/
            // 定义lua 脚本
            String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
            // 使用redis执行lua执行
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
            redisScript.setScriptText(script);
            // 设置一下返回值类型 为Long
            // 因为删除判断的时候，返回的0,给其封装为数据类型。如果不封装那么默认返回String 类型，
            // 那么返回字符串与0 会有发生错误。
            redisScript.setResultType(Long.class);
            // 第一个要是script 脚本 ，第二个需要判断的key，第三个就是key所对应的值。
            redisTemplate.execute(redisScript, Arrays.asList(locKey), uuid);
        } else {
            // 其他线程等待
            try {
                // 睡眠
                Thread.sleep(1000);
                // 睡醒了之后，调用方法。
                testLockLua();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean spikeMethod(String uid, String prodId){

        Boolean results = (Boolean) redisTemplate.execute(new SessionCallback<Boolean>() {
            @Override
            public Boolean execute(RedisOperations operations) throws DataAccessException {
                //1、uid和proId非空判断
                if(uid == null || prodId == null){
                    return false;
                }

                //2、拼接key
                //2.1 库存key
                String kcKey = "sk:"+prodId+":qt";

                //2.2 秒杀成功用户key
                String userKey = "sk:"+prodId+":user";
                //监视库存
                redisTemplate.watch(kcKey);

                //3、获取库存，如果库存为null，说明秒杀还没开始
                String kc = String.valueOf(redisTemplate.opsForValue().get(kcKey));
                if(kc == null){
                    System.out.println("秒杀还没开始，请等待！");
                    return false;
                }

                //4、判断当前用户是否重复秒杀操作
                if(redisTemplate.opsForSet().isMember(userKey,uid)){
                    System.out.println("已经秒杀成功，不能重复操作");
                    return false;
                }

                //5、判断如果商品库存数量 <= 0 ，秒杀结束
                if(Integer.parseInt(kc) <= 0){
                    System.out.println("秒杀已结束！");
                    return false;
                }
                //开启事务
                operations.multi();
                //组队操作
                //1、库存-1
                operations.opsForValue().decrement(kcKey);
                //2、把秒杀成功的用户加入到redis中
                operations.opsForSet().add(userKey, uid);
                //提交事务
                List<Object> results = operations.exec();
                if(results == null || results.size() == 0){
                    //秒杀成功
                    System.out.println("秒杀失败！货品已被抢光！");
                    return false;
                }
                System.out.println("秒杀成功！");
                return true;
            }
        });

        return results;
    }

    /**
     * 通过lua脚本解决库存遗留问题
     * @return
     */
    public boolean spikeByScript(String uid, String prodid){
        //System.err.println(luaScript());
        //设置键
        List<String> keys = new ArrayList<>();
        keys.add(uid);
        keys.add(prodid);

        //创建脚本执行器，使用当前redis模板
        DefaultScriptExecutor scriptExecutor = new DefaultScriptExecutor(redisTemplate);
        //创建脚本加载器，传入脚本字符串和返回值类型，redis中Integer对应java中的Long类型
        DefaultRedisScript defaultRedisScript = new DefaultRedisScript(luaScript(),Long.class);

        redisTemplate.setScriptExecutor(scriptExecutor);
        Object execute = redisTemplate.execute(defaultRedisScript, keys, uid,prodid);
        String flag = String.valueOf(execute);
        if("0".equals(flag)){
            System.out.println("已抢光！");
            return false;
        }

        if("1".equals(flag)){
            System.out.println("秒杀成功！");
            return true;
        }

        if("2".equals(flag)){
            System.out.println("不能重复操作！");
            return false;
        }
        return false;
    }




    //1、生成6位验证码
    public  String getCode(){
        Random random = new Random();
        String code = "";
        for (int i = 0; i < 6; i++) {
            int rand = random.nextInt(10);
            code += rand;
        }
        return code;
    }

    /**
     * lua脚本
     * @return
     */
    public String luaScript(){
        return "local userid=KEYS[1]; \n" +
                "local prodid=KEYS[2];\n" +
                "local qtkey=\"sk:\"..prodid..\":qt\";\n" +
                "local usersKey=\"sk:\"..prodid..\":user\";\n" +
                "local userExists=redis.call(\"sismember\",usersKey,userid);\n" +
                "if tonumber(userExists)==1 then \n" +
                "  return 2;\n" +
                "end\n" +
                "local num= redis.call(\"get\" ,qtkey);\n" +
                "if tonumber(num)<=0 then \n" +
                "  return 0; \n" +
                "else \n" +
                "  redis.call(\"decr\",qtkey);\n" +
                "  redis.call(\"sadd\",usersKey,userid);\n" +
                "end\n" +
                "return 1;";
    }
}
