// // /**

// //   根据tcp 通信时的不同操作指令创建不同的channel，不同的client相同的指令操作时通过同一个channel 发送
// //    结构(生产者)
// //    channel               exchange
// //  |==========|         |============|
// //  | channel  |========>| 		     |
// //  |==========|         |            |
// //                       |            |
// //  |==========|         |            |
// //  | channel  |========>| loop.direct|
// //  |==========|         |            |
// // 					    |            |
// //  |==========|         |            |
// //  | channel  |========>|            |
// //  |==========|         |============|

// // */
package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"blumipark-gw/util"

	logrus "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	v1 "gopkg.in/ini.v1"
)

//channel 类型
const (
	LOGIN   = "login"
	GROUP   = "group"
	GATEWAY = "gateway"
	POWER   = "power"
)

//模拟测试信息
type Info struct {
	Id       int    `json:"id" gorm:"column:id;auto_increment;primary_key"`
	Message  string `json:"message" gorm:"column:message"`
	ParentId string `json:"parentid" gorm:"column:parent_id"`
	Money    int    `json:"money" gorm:"column:money"`
}

type Channnel struct {
	//channel (发送信息)
	chal *amqp.Channel
	//ack chan(接收exchange 发送的ack)
	confirm chan amqp.Confirmation
	//err chan (错误接收管道)
	err chan *amqp.Error
	//接收退回消息的管道
	returnmessage chan amqp.Return
}

//redis 中信息存储
type GM struct {
	//client 发送的消息
	Info string `json:"info"`
	//发送的次数
	Num int
}

var (
	CHANNELTYPE = []string{LOGIN, GROUP, GATEWAY, POWER} //创建n个管道
	Exchange    = "loop.direct"                          //exchange
	sysmux      sync.WaitGroup
	ch          map[string]Channnel
	GlobalMux   sync.Mutex
	produceconn *amqp.Connection //生产则连接
	logs        *logrus.Logger   //日志
	cpu         *os.File
	mem         *os.File
	tryconnect  int    //重试连接次数
	Max         uint64 = 18446744073709551615
)

/**
链接rabbitmq 和初始化log 文件
*/
func init() {
	err := Connection()
	if err != nil {
		logs.Error(err)
		return
	}
	util.InitRedis()
	logs = logrus.New()

	//logs.SetFormatter(&logrus.JSONFormatter{})
	//日志文件
	filename := "rabbitmq_" + time.Now().Format("20060102") + ".log"
	files, _ := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	logs.Out = files
	ch = make(map[string]Channnel)

	//pprof 测试
	_, err = os.Stat("./cpu.pprof")
	if err == nil || os.IsExist(err) {
		os.Remove("./cpu.pprof")
	}
	cpu, _ = os.OpenFile("./cpu.pprof", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

}

/**
读取配置文件，连接rabbimq
*/
func Reconnection() error {
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
	produceconn, err = amqp.Dial(dial)
	if err != nil {
		log.Println("rabbit link fire err :", err)
	}
	return err

}

/**
连接rabbitmq,包括第一次连接和重新连接
*/
func Connection() error {
	GlobalMux.Lock()
	defer GlobalMux.Unlock()
	if produceconn == nil || produceconn.IsClosed() {
		if err := Reconnection(); err != nil {
			//第一次连接失败后定时尝试重新连接
			//超过三次则认定mq 服务挂掉(每次间隔20 秒)
			for {
				select {
				case <-time.After(20 * time.Second):
					log.Printf("尝试第:%d次连接服务", tryconnect)
					err := Reconnection()
					if err == nil {
						//重置conn
						sysmux.Add(len(CHANNELTYPE))
						for _, v := range CHANNELTYPE {
							go ChannelConfig(v, "direct")
						}
						sysmux.Wait()
						//重置事件监听
						log.Println("rabbitmq serve 连接成功")
						tryconnect = 0
						//将redis中消息进行消费
						go pop()
						return nil
					} else if tryconnect == 3 {
						return errors.New("rabbitmq server conn fail")
					}
					tryconnect++
				}
			}
		}

	}
	return nil
}

// 初始化生产者channel
func Produce() error {
	//创建len(CHANNELTYPE) 个 channel
	sysmux.Add(len(CHANNELTYPE))
	for _, v := range CHANNELTYPE {
		go ChannelConfig(v, "direct")
	}
	sysmux.Wait()
	//如果exchange 不存在，则创建
	//exchange 名称
	//exchange 类型
	//是否持久化
	//是否自动删除
	//当您不希望内部交换被公开时，代理的内部交换是有用的。
	//是否不需要等待服务器确认
	if err := ch[LOGIN].chal.ExchangeDeclare(Exchange, "direct", true, false, false, false, nil); err != nil {
		fmt.Println("create exchange fail,err :", err)
		return errors.New("ExchangeDeclare fail")
	}
	//创建死信交换机
	if err := ch[LOGIN].chal.ExchangeDeclare("death", "direct", true, false, false, false, nil); err != nil {
		fmt.Println("create death exchange fail,err :", err)
		return errors.New("ExchangeDeclare fail")
	}
	//pprof 测试
	pprof.StartCPUProfile(cpu)
	defer pprof.StopCPUProfile()
	go pop()

	return nil
}

//监听推送失败的消息管道或者是管道错误
func Emit(channel Channnel) {
	for {
		select {
		//接收exchange 返回的消息
		case info := <-channel.returnmessage:
			log.Println("读取消息:", string(info.Body))
			SendMessage(info.Body, LOGIN, 1)
		case t := <-channel.err:
			log.Println("处理错误，关闭通道", t)
			//如果retuenmessage 管道中存在消息，则先将消息全部读出
			if len(channel.returnmessage) > 0 {
				for i := 0; i < len(channel.returnmessage); i++ {
					result := <-channel.returnmessage
					SendMessage(result.Body, LOGIN, 1)
				}
			}
			errCheck(t, LOGIN)
			return
		}
	}
}

//初始化所有的channel
func ChannelConfig(channeltype string, kind string) {
	GlobalMux.Lock()
	defer GlobalMux.Unlock()
	//创建一个channel
	channel, _ := produceconn.Channel()

	//是否不需要等待服务器确认
	channel.Confirm(false)
	//ack管道(用于判断message 是否被exchange 接收)
	confirm := channel.NotifyPublish(make(chan amqp.Confirmation, 20))
	//错误管道(监听channel关闭)
	errchan := channel.NotifyClose(make(chan *amqp.Error, 20))
	//接收被exchange 返回到消息（当kind 不是在exchange 的所有queue中其中之一时）
	returnmessage := channel.NotifyReturn(make(chan amqp.Return, 20))
	ch[channeltype] = Channnel{channel, confirm, errchan, returnmessage}

	//给初始化的confirm发送20个数据
	for k := 0; k < 20; k++ {
		Max = Max - 1
		ch[channeltype].confirm <- amqp.Confirmation{Ack: true, DeliveryTag: Max}
	}
	go Emit(ch[channeltype])

	sysmux.Done()
}

/**
produce 通过channel 推送消息
*/
func CheckAck(message []byte, channeltype string, key string, cfm amqp.Confirmation, ok bool, num int) {
	channel := ch[channeltype]
	if cfm.Ack && ok {
		//推送消息到exchange
		err := channel.chal.Publish(Exchange, key, true, false, amqp.Publishing{
			//生产message id,为确保id唯一,messageid = time + channelname + DeliveryTag
			//20200910-10:00:00.login.1
			//20200910-10:00:00.group.1
			MessageId:    time.Now().Format("20060102-03:04:05") + "." + channeltype + "." + strconv.FormatUint(cfm.DeliveryTag, 10),
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         message,
		})

		if err != nil {
			push(message, num+1)
			errCheck(err, channeltype)
			ch[channeltype].confirm <- amqp.Confirmation{Ack: true}
		}
	} else if !ok {
		log.Println("通道关闭，消息发送失败")
		push(message, num)
		return
	}

}

//发送信息
func SendMessage(message []byte, key string, num int) {
	//检查conn 连接是否正常,如果rabbitmq连接关闭，则将消息暂存于redis 中
	if produceconn != nil && !produceconn.IsClosed() {
		select {
		case cfm, ok := <-ch[GROUP].confirm:
			CheckAck(message, GROUP, key, cfm, ok, num)
		case cfm, ok := <-ch[LOGIN].confirm:
			CheckAck(message, LOGIN, key, cfm, ok, num)
		}
	} else {
		//当rabbitmq serve kill 时，将等到重新连接到rabbitmq 这段时间内到消息暂存与redis中
		err := push(message, num)
		//尝试重新连接rabbitmq
		if err != nil {
			log.Println("消息存储失败")
		} else {
			log.Println("消息存储成功")
		}
		//尝试重新连接
		Connection()
	}
}

//错误处理
func errCheck(err interface{}, channeltype string) {
	switch v := err.(type) {
	case *amqp.Error:
		switch v.Code {
		//管道被close掉
		case amqp.ChannelError:
			fmt.Println("channnel close:", v)
			ChannelConfig(channeltype, "direct")
		case amqp.SyntaxError:
		//rabbitmq 服务 被 close 掉,尝试重新连接
		case amqp.FrameError, amqp.ConnectionForced:
			fmt.Println("rabbitmq 服务挂掉,等待重启", err)
			Connection()
		case amqp.NotFound:
			fmt.Println("没有交换机")
		default:
			fmt.Println("其他错误:", err)
		}

	}
}

//将推送失败的消息或者因为mq 服务挂掉的原因无法推送的消息存入redis
func push(message []byte, num int) error {
	bs, _ := json.Marshal(GM{Info: string(message), Num: num})
	_, err := util.Push("queue", string(bs))
	if err != nil {
		logs.Error("消息存储失败,错误原因:", err)
		return err
	}
	return nil
}

//从redis中取出消息
func pop() {
	var gm GM
	for {
		result, err := util.Pop("queue")
		bys := []byte(result)
		if result == "" {
			fmt.Println("没有数据")
			return
		}
		err = json.Unmarshal(bys, &gm)
		if err != nil {
			log.Println("消息放序列化失败:", err)
			break
		}

		if gm.Num >= 2 {
			fmt.Println("消息重发次数到上限")
			break
		}
		go SendMessage(bys, LOGIN, gm.Num)
	}
}
