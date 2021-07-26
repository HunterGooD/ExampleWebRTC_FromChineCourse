package room

import (
	"conference/media"
	"conference/server"
	"conference/util"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/pion/webrtc/v2"
)

const (
	MethodJoin        = "join"
	MethodLeave       = "leave"
	MethodPublish     = "publish"
	MethodSubscribe   = "subscribe"
	MethodOnPublish   = "onPublish"
	MethodOnSubscribe = "onSubscribe"
	MethodOnUnpublish = "onUnpublish"
)

func (roomManager *RoomManager) HandleNewWebSocket(conn *server.WebSocketConn, request *http.Request) {
	util.Infof("On Open %v", request)
	//监听消息事件
	conn.On("message", func(message []byte) {
		//解析Json数据
		request, err := util.Unmarshal(string(message))

		if err != nil {
			util.Errorf("解析Json数据Unmarshal错误 %v", err)
			return
		}

		var data map[string]interface{} = nil

		tmp, found := request["data"]
		if !found {
			util.Errorf("没有发现数据")
			return
		}

		data = tmp.(map[string]interface{})

		roomId := data["roomId"].(string)
		util.Infof("房间Id:%v", roomId)

		room := roomManager.getRoom(roomId)

		if room == nil {
			room = roomManager.createRoom(roomId)
		}

		userID := data["userID"].(string)
		user := room.GetUser(userID)
		if user == nil {
			user = NewUser(userID, conn)
		}

		switch request["type"] {
		case MethodJoin:
			processJoin(user, data, roomManager)
			break
		case MethodPublish:
			processPublish(user, data, roomManager)
			break
		case MethodSubscribe:
			processSubscribe(user, data, roomManager)
			break
		case MethodLeave:
			processJoin(user, data, roomManager)
			break
		default:
			{
				util.Warnf("未知的请求 %v", request)
			}
			break
		}

	})

	conn.On("close", func(code int, text string) {
		util.Infof("连接关闭 %v", conn)
		var userID string = ""
		var roomId string = ""

		for _, room := range roomManager.rooms {
			for _, user := range room.users {
				if user.conn == conn {
					userID = user.ID()
					roomId = room.ID
					break
				}
			}
		}

		if roomId == "" {
			util.Errorf("没有查找到退出的房间及用户")
			return
		}
		processLeave(roomId, userID, roomManager)
	})
}

func processLeave(roomId, userID string, roomManager *RoomManager) {

	room := roomManager.getRoom(roomId)
	if room == nil {
		return
	} else {

		onUnpublish := make(map[string]interface{})
		onUnpublish["pubID"] = userID

		for id, user := range room.users {
			if id != userID {
				user.sendMessage(MethodOnUnpublish, onUnpublish)
			}
		}
		room.delWebRTCPeer(userID, true)
		room.delWebRTCPeer(userID, false)
		room.DeleteUser(userID)
	}
}

func processJoin(user *User, message map[string]interface{}, roomManager *RoomManager) {
	roomId := message["roomId"]
	if roomId == nil {
		return
	}

	room := roomManager.getRoom(roomId.(string))
	if room == nil {
		room = roomManager.createRoom(roomId.(string))
	}

	room.AddUser(user)
	onPublish := make(map[string]interface{})

	room.pubPeerLock.RLock()
	defer room.pubPeerLock.RUnlock()
	//找到当前房间的所有发布者
	for peerId, _ := range room.pubPeers {
		if peerId != user.ID() {
			onPublish["pubID"] = peerId
			onPublish["userID"] = peerId
			room.GetUser(user.ID()).sendMessage(MethodOnPublish, onPublish)
		}
	}

	onJoinData := make(map[string]interface{})
	onJoinData["status"] = "success"
	user.sendMessage("onJoinRoom", onJoinData)
	log.Print("onJoinRoom")
}

func processPublish(user *User, message map[string]interface{}, roomManager *RoomManager) {
	if message["jsep"] == nil {
		log.Print("jsep...")
		return
	}
	j := message["jsep"].(map[string]interface{})
	if j["sdp"] == nil {
		log.Print("sdp...")
		return
	}

	roomId := message["roomId"]
	r := roomManager.getRoom(roomId.(string))
	if r == nil {
		log.Print("room...")
		return
	}
	r.addWebRTCPeer(user.ID(), true)
	jsep := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  j["sdp"].(string),
	}
	answer, err := r.answer(user.ID(), "", jsep, true)
	if err != nil {
		log.Print("创建Answer失败")
		return
	}

	resp := make(map[string]interface{})
	resp["jsep"] = answer
	resp["userID"] = user.ID()
	respByte, err := json.Marshal(resp)
	if err != nil {
		return
	}
	respStr := string(respByte)
	if respStr != "" {
		//返回给自己jsep
		user.sendMessage(MethodOnPublish, resp)

		onPublish := make(map[string]interface{})
		onPublish["pubID"] = user.ID()
		//发送给房间其他人jsep
		r.sendMessage(user, MethodOnPublish, resp)
		return
	}

}

func processSubscribe(user *User, message map[string]interface{}, roomManager *RoomManager) {
	if message["jsep"] == nil {
		log.Print("jsep...")
		return
	}
	j := message["jsep"].(map[string]interface{})
	if j["sdp"] == nil {
		log.Print("sdp...")
		return
	}

	roomId := message["roomId"]
	r := roomManager.getRoom(roomId.(string))
	if r == nil {
		log.Print("room...")
		return
	}

	r.addWebRTCPeer(user.ID(), false)
	jsep := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  j["sdp"].(string),
	}
	answer, err := r.answer(user.ID(), message["pubID"].(string), jsep, false)
	if err != nil {
		log.Print("创建Answer失败")
		return
	}

	resp := make(map[string]interface{})
	resp["jsep"] = answer
	resp["userID"] = user.ID()
	resp["pubID"] = message["pubID"]

	respByte, err := json.Marshal(resp)
	if err != nil {
		log.Print(err.Error())
		return
	}
	r.sendPLI(user.ID())
	respStr := string(respByte)

	if respStr != "" {
		//返回给自己jsep
		user.sendMessage(MethodOnSubscribe, resp)
		log.Printf("Subscribe返回给自己的Id:%s", user.ID())
		return
	}

}

type RoomManager struct {
	rooms map[string]*Room
}

func NewRoomManager() *RoomManager {
	var roomManager = &RoomManager{
		rooms: make(map[string]*Room),
	}
	return roomManager
}

type Room struct {
	users map[string]*User

	ID string

	pubPeers    map[string]*media.WebRTCPeer
	subPeers    map[string]*media.WebRTCPeer
	pubPeerLock sync.RWMutex
	subPeerLock sync.RWMutex
}

func NewRoom(id string) *Room {
	var room = &Room{
		users:    make(map[string]*User),
		pubPeers: make(map[string]*media.WebRTCPeer),
		subPeers: make(map[string]*media.WebRTCPeer),
		ID:       id,
	}
	return room
}

func (roomManager *RoomManager) getRoom(id string) *Room {
	return roomManager.rooms[id]
}

func (roomManager *RoomManager) createRoom(id string) *Room {
	roomManager.rooms[id] = NewRoom(id)
	return roomManager.rooms[id]
}

func (roomManager *RoomManager) deleteRoom(id string) {
	delete(roomManager.rooms, id)
}

func (room *Room) AddUser(newUser *User) {
	room.users[newUser.ID()] = newUser
}

func (room *Room) GetUser(userID string) *User {

	if user, ok := room.users[userID]; ok {
		return user
	}
	return nil
}

func (room *Room) DeleteUser(userID string) {
	delete(room.users, userID)
}

func (room *Room) getWebRTCPeer(id string, sender bool) *media.WebRTCPeer {
	if sender {
		room.pubPeerLock.Lock()
		defer room.pubPeerLock.Unlock()
		return room.pubPeers[id]
	} else {
		room.subPeerLock.Lock()
		defer room.subPeerLock.Unlock()
		return room.subPeers[id]
	}
}

func (r *Room) delWebRTCPeer(id string, sender bool) {
	if sender {
		r.pubPeerLock.Lock()
		defer r.pubPeerLock.Unlock()
		if r.pubPeers[id] != nil {
			if r.pubPeers[id].PC != nil {
				r.pubPeers[id].PC.Close()
			}
			r.pubPeers[id].Stop()
		}
		delete(r.pubPeers, id)
	} else {
		r.subPeerLock.Lock()
		defer r.subPeerLock.Unlock()
		if r.subPeers[id] != nil {
			if r.subPeers[id].PC != nil {
				r.subPeers[id].PC.Close()
			}
			r.subPeers[id].Stop()
		}
		delete(r.subPeers, id)
	}

}

func (room *Room) addWebRTCPeer(id string, sender bool) {
	if sender {
		room.pubPeerLock.Lock()
		defer room.pubPeerLock.Unlock()
		if room.pubPeers[id] != nil {
			room.pubPeers[id].Stop()
		}
		room.pubPeers[id] = media.NewWebRTCPeer(id)
	} else {
		room.subPeerLock.Lock()
		defer room.subPeerLock.Unlock()
		if room.subPeers[id] != nil {
			room.subPeers[id].Stop()
		}
		room.subPeers[id] = media.NewWebRTCPeer(id)
	}
}

//关键侦丢包重传
func (r *Room) sendPLI(skipID string) {
	log.Print("Room.sendPLI")
	r.pubPeerLock.RLock()
	defer r.pubPeerLock.RUnlock()
	for k, v := range r.pubPeers {
		if k != skipID {
			v.SendPLI()
		}
	}
}

func (room *Room) sendMessage(from *User, msgType string, data map[string]interface{}) {

	var message map[string]interface{} = nil

	message = map[string]interface{}{
		"type": msgType,
		"data": data,
	}

	for id, user := range room.users {
		if id != from.ID() {
			user.conn.Send(util.Marshal(message))
		}
	}

}

func (r *Room) answer(id string, pubID string, offer webrtc.SessionDescription, sender bool) (webrtc.SessionDescription, error) {

	p := r.getWebRTCPeer(id, sender)

	var err error
	var answer webrtc.SessionDescription
	if sender {
		answer, err = p.AnswerSender(offer)
	} else {
		r.pubPeerLock.RLock()

		pub := r.pubPeers[pubID]
		r.pubPeerLock.RUnlock()
		ticker := time.NewTicker(time.Millisecond * 2000)
		for {
			select {
			case <-ticker.C:
				goto ENDWAIT
			default:
				if pub.VideoTrack == nil || pub.AudioTrack == nil {
					time.Sleep(time.Millisecond * 100)
				} else {
					goto ENDWAIT
				}
			}
		}

	ENDWAIT:
		answer, err = p.AnswerReceiver(offer, &pub.VideoTrack, &pub.AudioTrack)

	}
	return answer, err

}

func (r *Room) Close() {
	r.pubPeerLock.Lock()
	defer r.pubPeerLock.Unlock()
	for _, v := range r.pubPeers {
		if v != nil {
			v.Stop()
			if v.PC != nil {
				v.PC.Close()
			}
		}
	}
}
