package rabbitmq

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"time"

	"github.com/streadway/amqp"
	v1 "gopkg.in/ini.v1"
)

var (
	ConsumerChannnel map[string][]*amqp.Channel
	errchan          chan interface{}
	Consumerconn     *amqp.Connection
	expiration_time  int
)

const (
	DEATH_LOGIN = "death_queue_login"
	DEATH_GROUP = "death_queue_GROUP"
)

func init() {
	errchan = make(chan interface{}, 1)
	ConsumerChannnel = make(map[string][]*amqp.Channel)
	file, err := v1.Load("./conf.ini")
	if err != nil {
		log.Fatal("rabbit config file err:", err)
	}
	selection := file.Section("redis")
	expiration_time, _ = selection.Key("expiration_time").Int()

}

//创建连接
func ReconnectionConsumer() error {
	file, err := v1.Load("./conf.ini")
	if err != nil {
		log.Fatal("rabbit config file err:", err)
	}
	selection := file.Section("rabbitmq")
	username := selection.Key("username").String()
	password := selection.Key("password").String()
	host := selection.Key("host").String()
	port := selection.Key("port").String()
	dial := "amqp://" + username + ":" + password + "@" + host + ":" + port
	Consumerconn, err = amqp.Dial(dial)
	if err != nil {
		log.Println("rabbit link fire err :", err)
	}
	return err

}

//创建管道
func createqueue(v string) (*amqp.Channel, error) {
	channel, _ := Consumerconn.Channel()
	//如果队列不存在，则创建
	//1. 队列名称
	//2. 是否持久化, 队列的声明默认是存放到内存中的，如果rabbitmq重启会丢失，如果想重启之后还存在就要使队列持久化，
	//3. 是否自动删除,当最后一个消费者断开连接之后队列是否自动被删除，可以通过RabbitMQ Management，查看某个队列的消费者数量，
	//当consumers = 0时队列就会自动删除
	//4. 是否排外，当连接关闭时connection.close()该队列是否会自动删除；二：该队列是否是私有的private，
	//如果不是排外的，可以使用两个消费者都访问同一 个队列，没有任何问题，如果是排外的，会对当前队列加锁，其他通道channel是不能访问的，如果强制访问会报异常
	table := make(amqp.Table)
	//设置死信交换机
	table["x-dead-letter-exchange"] = "death"
	//设置路由
	table["x-dead-letter-routing-key"] = v
	//设置过期时间
	table["x-message-ttl"] = 1000000
	if _, err := channel.QueueDeclare(v, true, false, false, false, table); err != nil {
		log.Println("channerl QueueDeclare err:", err)
		return nil, err
	}

	channel.QueueBind(v, v, Exchange, false, nil)
	channel.Qos(3, 0, true)
	return channel, nil
}

// 创建管道和绑定到交换机
func initqueue() error {
	for _, v := range CHANNELTYPE {
		for i := 0; i < 10; i++ {
			//创建消费者
			channel, err := createqueue(v)
			if err != nil {
				fmt.Println("创建失败")
				return errors.New("create queue fail")
			}
			ConsumerChannnel[v] = append(ConsumerChannnel[v], channel)
		}
	}
	return nil
}

//消费者消费
func Consumer(k string, ch *amqp.Channel, consumer string) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("捕获到异常，程序结束")
			//重新创建channel
			chanel, _ := createqueue(k)
			//关闭原来到管道
			ch.Close()
			//替换新的consumer
			ch = chanel
			//重启consuemr
			go Consumer(k, ch, consumer)

		}
	}()

	reuslt, err := ch.Consume(k, consumer, false, false, false, false, nil)
	if err != nil {
		log.Printf(" %s  Consume init error: %s", k, err)
		return
	}
	messageprocessing(reuslt, ch)
}

//消息处理
func messageprocessing(reuslt <-chan amqp.Delivery, ch *amqp.Channel) {
	for {
		select {
		case data := <-reuslt:
			// 业务逻辑
			// 防止重复消费
			bl, err := util.ExistsRedisValue(data.MessageId)
			fmt.Println("messgae===", string(data.Body), "=====", data.MessageId)
			if err == nil && !bl {
				// handle.OnMessage(data.Body, nil)
				// 测试模拟插入数据
				// err = Insert(data.Body, data.MessageId, expiration_time)
				// if err == nil {
				data.Ack(false)
				// } else {
				// 	fmt.Println("数据插入失败,原因：", err)
				// 	ch.Nack(data.DeliveryTag, false, false)
				// }
			}
			if bl {
				data.Ack(false)
			}
		}
	}
}

//启动消费者
func ConsumerStart() error {
	ReconnectionConsumer()
	if err := initqueue(); err != nil {
		return err
	}

	for k, v := range ConsumerChannnel {
		for i := 0; i < 1; i++ {
			go Consumer(k, v[i], strconv.Itoa(i))
		}
	}
	//查看当前goroutine 数量
	go func() {
		for {
			select {
			case <-time.After(1 * time.Second):
				log.Println("当前goroutine 数量:", runtime.NumGoroutine())
			}
		}
	}()
	if err := DeathExchane(DEATH_LOGIN, LOGIN, "death_consumer_login"); err != nil {
		return err
	}

	if err := DeathExchane(DEATH_GROUP, GROUP, "death_consumer_group"); err != nil {
		return err
	}
	log.Println("开启消费.....")
	return nil

}

//创建死信队列
func DeathExchane(queuename string, key string, consumer string) error {
	ch, err := Consumerconn.Channel()
	if err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(queuename, true, false, false, false, nil); err != nil {
		return err
	}

	if err := ch.QueueBind(queuename, key, "death", false, nil); err != nil {
		return err
	}
	go Consumer(key, ch, consumer)
	return nil
}
