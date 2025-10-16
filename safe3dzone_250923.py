## License: Apache 2.0. See LICENSE file in root directory.
## Copyright(c) 2015-2017 Intel Corporation. All Rights Reserved.

###############################################
##      Open CV and Numpy integration        ##
###############################################
import pyrealsense2 as rs
import numpy as np
import cv2
import imageio
import os
import shutil

#from ultralytics.utils import yaml_load   # not working! so make one.
#----------------------------
# 1)ultralytics yaml error handling
#----------------------------
from ultralytics import YOLO
from deep_sort_realtime.deepsort_tracker import DeepSort
from ultralytics.utils.checks import check_yaml
import yaml
def yaml_load(f1):
    with open(f1) as f:
        file = yaml.full_load(f)
    return file['names']
def make_clean_folder(path_folder):
    if not os.path.exists(path_folder):
        os.makedirs(path_folder)
    else:
        user_input = input("%s not empty. Overwrite? (y/n) : " % path_folder)
        if user_input.lower() == "y":
            shutil.rmtree(path_folder)
            os.makedirs(path_folder)
        else:
            exit()
#timezone for unixtime sending
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # Python 3.8 이하

from multiprocessing import shared_memory
import warnings
warnings.filterwarnings('ignore')
import threading
import logging
import json
import time
from kafka import KafkaProducer

#----------------------------
# 2)Logging 설정
#----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("kafka-infer")

#----------------------------
# 3) Kafka 설정
#----------------------------
TOPIC_PUBLISH = 'DGSP-ZONE-INFERENCE-TAPO1'
KAFKA_SERVER = 'piai_kafka2.aiot.town:9092'
log.info("KafkaConsumer 설정 중...")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
#----------------------------
# 4) 공유 버퍼 & 락
#----------------------------
lock = threading.Lock()
TARGET_WIDTH = 640
TAREGET_HEIGHT= 480
TARGET_DEPTH = 3
#sample_array = np.zeros((TAREGET_HEIGHT, TARGET_WIDTH, TARGET_DEPTH), dtype=np.uint8)  # (600768,)
#stream_shm = shared_memory.SharedMemory(name ='shm',create=True, size=sample_array.nbytes)
cap_shape=(TAREGET_HEIGHT, TARGET_WIDTH, TARGET_DEPTH)
stream_shm = np.zeros(cap_shape, dtype=np.uint8)  # (600768,)
shared_a = np.ndarray(cap_shape, dtype=np.uint8, buffer=stream_shm)
#----------------------------
# 5) GLib
#----------------------------
import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstRtspServer', '1.0')
from gi.repository import Gst, GstRtspServer, GLib, GObject
GObject.threads_init()
Gst.init(None)

######################################################
# 6) Vertex setting UI.  Check for number + arrow combinations
#######################################################
from pynput import keyboard
import json
with open("parameters.json", "r") as f:
    loaded_params = json.load(f)

pp1 = loaded_params['pp1'] # same x,z as p6
pp2 = loaded_params['pp2'] # same x,z as p7
pp3 = loaded_params['pp3']  # same x,z as p8
pp4 = loaded_params['pp4']  # same x,z as p9
#p6: down p1, p7: down p2, p8: down p3, p9: down p4
pp6 = loaded_params['pp6'] 
pp7 = loaded_params['pp7']    #
pp8 = loaded_params['pp8']    #
pp9 = loaded_params['pp9'] 

pressed_keys = set()
def on_press(key):
    pressed_keys.add(key)
    for digit in map(str, range(1, 10)):
        try:
            if keyboard.KeyCode.from_char(digit) in pressed_keys and keyboard.Key.up  in pressed_keys:
                #log.info(f"{digit} {type(digit)}+ ↑ Up")
                digit=int(digit)
                if digit==1: pp1[1]-=1; print("PP1",pp1)
                elif digit==2:pp2[1]-=1; print("PP2",pp2)
                elif digit==3:pp3[1]-=1; print("PP3",pp3)
                elif digit==4:pp4[1]-=1; print("PP4",pp4)
                elif digit==6:pp6[1]-=1; print("PP6",pp6)
                elif digit==7:pp7[1]-=1; print("PP7",pp7)
                elif digit==8:pp8[1]-=1; print("PP8",pp8)
                elif digit==9:pp9[1]-=1; print("PP9",pp9)
                elif digit==5:
                    params = {**{f"pp{i}": globals()[f"pp{i}"] for i in range(1, 5)}, **{f"pp{i}": globals()[f"pp{i}"] for i in range(6, 10)}}
                    with open("parameters.json", "w") as f:
                        json.dump(params, f, indent=4)
                    print("Saved pp1 to pp9 into parameters.json")
                else:
                    log.info(f"key error with up {digit}")
            elif keyboard.KeyCode.from_char(digit) in pressed_keys and keyboard.Key.down in pressed_keys:
                #log.info(f"{digit} + ↓ Down")
                digit=int(digit)
                if digit==1: pp1[1]+=1; print("PP1",pp1)
                elif digit==2:pp2[1]+=1; print("PP2",pp2)
                elif digit==3:pp3[1]+=1; print("PP3",pp3)
                elif digit==4:pp4[1]+=1; print("PP4",pp4)
                elif digit==6:pp6[1]+=1; print("PP6",pp6)
                elif digit==7:pp7[1]+=1; print("PP7",pp7)
                elif digit==8:pp8[1]+=1; print("PP8",pp8)
                elif digit==9:pp9[1]+=1; print("PP9",pp9)
                else:
                    log.info(f"key error with down {digit}")
            elif keyboard.KeyCode.from_char(digit) in pressed_keys and keyboard.Key.left  in pressed_keys:
                #log.info(f"{digit} + ← Left")
                digit=int(digit)
                if digit==1: pp1[0]-=1; print("PP1",pp1)
                elif digit==2:pp2[0]-=1; print("PP2",pp2)
                elif digit==3:pp3[0]-=1; print("PP3",pp3)
                elif digit==4:pp4[0]-=1; print("PP4",pp4)
                elif digit==6:pp6[0]-=1; print("PP6",pp6)
                elif digit==7:pp7[0]-=1; print("PP7",pp7)
                elif digit==8:pp8[0]-=1; print("PP8",pp8)
                elif digit==9:pp9[0]-=1; print("PP9",pp9)
                else:
                    log.info(f"key error with left {digit}")
            elif keyboard.KeyCode.from_char(digit) in pressed_keys and keyboard.Key.right in pressed_keys:
                #log.info(f"{digit} + → Right")
                digit=int(digit)
                if digit==1: pp1[0]+=1; print("PP1",pp1)
                elif digit==2:pp2[0]+=1; print("PP2",pp2)
                elif digit==3:pp3[0]+=1; print("PP3",pp3)
                elif digit==4:pp4[0]+=1; print("PP4",pp4)
                elif digit==6:pp6[0]+=1; print("PP6",pp6)
                elif digit==7:pp7[0]+=1; print("PP7",pp7)
                elif digit==8:pp8[0]+=1; print("PP8",pp8)
                elif digit==9:pp9[0]+=1; print("PP9",pp9)
                else:
                    log.info(f"key error with right {digit}")

        except AttributeError:
            continue

def on_release(key):
    pressed_keys.discard(key)
    # Stop listener on Esc
    if key == keyboard.Key.esc:
        return False
    
def keyboard_thread():
    with keyboard.Listener(on_press=on_press, on_release=on_release) as listener:
        listener.join()

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

################################################################################################
#  7) Zone fencing logic
################################################################################################
def plane_equation_from_points(p1, p2, p3):
    # Convert points to numpy arrays
    p1, p2, p3 = np.array(p1), np.array(p2), np.array(p3)
    v1 = p2 - p1
    v2 = p3 - p1

    normal = np.cross(v1, v2)
    a, b, c = normal
    d = -np.dot(normal, p1)
    return a, b, c, d

################################################################################################
#  8) Initialize MediaPipe Pose and Drawing utilities
################################################################################################\
import mediapipe as mp
mp_pose = mp.solutions.pose
mp_drawing = mp.solutions.drawing_utils
pose = mp_pose.Pose(static_image_mode=False)
my_pose = {
    0: "nose",
    2:	"Left Eye",
    5:	"Right Eye",
    7:	"Left Ear",
    8:	"Right Ear",
    9:	"Mouth Left",
    10:	"Mouth Right",
    11: "left_shoulder",
    12: "right_shoulder",
    #13: "left_elbow",
    #14: "right_elbow",
    # 15: "left_wrist",
    # 16: "right_wrist",
    23: "left_hip",
    24: "right_hip",
    # 25: "left_knee",
    # 26: "right_knee",
    # 27: "left_ankle",
    # 28: "right_ankle",
    # ... add all if you want
}
VISIBILITY_THRESHOLD = 0.8 # Visibility threshold — tweak as needed
################################################################################################
#  9) video file writing when safe zone violated
################################################################################################
from collections import deque
MAX_FRAMES=10
frame_buffer = deque(maxlen=MAX_FRAMES)
MAX_VIOLATE = 10
MAX_NONVIOLATE = 60

fourcc = cv2.VideoWriter_fourcc(*'XVID')  # Or 'mp4v' for MP4
out = cv2.VideoWriter('./output.avi', fourcc, 20.0, (640, 480))
################################################################################################
#  10) Robotic Arm movement detection
################################################################################################



################################################################################################
#  11) s_3dzone_thread: rs 3d camera pipeline + yolo11 + zone-fencing inference + shared_memory
################################################################################################
def safe3dzone_thread():
    log.info("[s_3dzone_thread] 3dzone thread 시작")
    ############################################################################
    # 10.1.1 yolo11 setting, yaml file failure handling  
    ############################################################################
    model = YOLO("./yolo11n.pt")
    #CLASSES = yaml_load('./coco128.yaml')['names']
    CLASSES = yaml_load('./coco128.yaml')     
    colors = np.random.uniform(0, 255, size=(len(CLASSES), 3))
    ############################################################################
    # 10.1.2 yolo11 setting for Marker Tracking  
    ############################################################################
    marker_model = YOLO("./yolo11n_custom.pt")
    tracker = DeepSort(max_age=60)
    det_running = 0
    ############################################################################
    # 10.2 realsense sdk setting:  Configure depth and color streams
    ############################################################################
    #make_clean_folder("./rs_streamdata/")
    pipeline = rs.pipeline()
    config = rs.config()

    # Get device product line for setting a supporting resolution
    pipeline_wrapper = rs.pipeline_wrapper(pipeline)
    pipeline_profile = config.resolve(pipeline_wrapper)
    device = pipeline_profile.get_device()
    device_product_line = str(device.get_info(rs.camera_info.product_line))

    found_rgb = False
    for s in device.sensors:
        if s.get_info(rs.camera_info.name) == 'RGB Camera':
            found_rgb = True
            break
    if not found_rgb:
        print("The demo requires Depth camera with Color sensor")
        exit(0)

    config.enable_stream(rs.stream.depth, 640, 480, rs.format.z16, 30)
    config.enable_stream(rs.stream.color, 640, 480, rs.format.bgr8, 30)

    # Start rs streaming
    pipeline.start(config)
    print("Start realsense streaming pumping! ")
   
    try:
        sys_time = int(time.time())-32400   #init. kafka sending interval time
        edge_time = sys_time                #init edge device inference time
        violate_cnt, no_violate_cnt, in_writing = 0, 0, False  #init. safe zone violation rule

        while True:
            frames = pipeline.wait_for_frames() # Wait for a coherent pair of frames: depth and color

            depth_frame = frames.get_depth_frame()
            color_frame = frames.get_color_frame()
            if not depth_frame or not color_frame:
                continue

            # Convert images to numpy arrays : width:640, height:480. 
            # NOTE: depth_image[height, width] & color_image [height, width]. NOTE:[480, 640] is out of bound
            depth_image = np.asanyarray(depth_frame.get_data())             
            color_image = np.asanyarray(color_frame.get_data())
            #print(f"{depth_image[0, 0]}, {depth_image[479, 639]}") 

            ############################################################################
            # 10.1 Set s-points Here : p1: the closest ptr, p2: right, p3: left, p4: behind p1 
            ############################################################################

            # pp1[2],pp2[2],pp3[2],pp4[2],pp6[2],pp7[2],pp8[2],pp9[2] = \
            #     int(depth_image[pp1[1], pp1[0]]), int(depth_image[pp2[1], pp2[0]]), int(depth_image[pp3[1], pp3[0]]),int(depth_image[pp4[1], pp4[0]]), \
            #     int(depth_image[pp6[1], pp6[0]]), int(depth_image[pp7[1], pp7[0]]), int(depth_image[pp8[1], pp8[0]]),int( depth_image[pp9[1], pp9[0]])
            # Set 6,7,8,9 depth using 1,2,3,4 depth
            pp1[2],pp2[2],pp3[2],pp4[2],pp6[2],pp7[2],pp8[2],pp9[2] = \
                int(depth_image[pp1[1], pp1[0]]), int(depth_image[pp2[1], pp2[0]]), int(depth_image[pp3[1], pp3[0]]),int(depth_image[pp4[1], pp4[0]]), \
                int(depth_image[pp1[1], pp1[0]]), int(depth_image[pp2[1], pp2[0]]), int(depth_image[pp3[1], pp3[0]]),int(depth_image[pp4[1], pp4[0]])
            
            #print(depth_frame.get_distance(p1[0], p1[1])) #4.15500020980835  meter!
            # print(f"p1: {pp1}")   #4155  mm!!
            p1 = pp1[:2]
            p2 = pp2[:2]
            p3 = pp3[:2]
            p4 = pp4[:2]
            p6 = pp6[:2]
            p7 = pp7[:2]
            p8 = pp8[:2]
            p9 = pp9[:2]

            a1,b1,c1,d1 = plane_equation_from_points(pp1,pp2,pp6)
            a2,b2,c2,d2 = plane_equation_from_points(pp1,pp3,pp6)
            a3,b3,c3,d3 = plane_equation_from_points(pp2,pp4,pp7)
            a4,b4,c4,d4 = plane_equation_from_points(pp3,pp4,pp8)

            # Draw a filled circle with a small radius to represent a point
            color1 = (0, 0, 255)
            cv2.circle(color_image, p1, radius=3, color=color1, thickness=-1)
            cv2.circle(color_image, p2, radius=3, color=color1, thickness=-1)
            cv2.circle(color_image, p3, radius=3, color=color1, thickness=-1)
            cv2.circle(color_image, p4, radius=3, color=color1, thickness=-1)
            cv2.circle(color_image, p6, radius=3, color=color1, thickness=-1)
            cv2.circle(color_image, p7, radius=3, color=color1, thickness=-1)
            cv2.circle(color_image, p8, radius=3, color=color1, thickness=-1)
            cv2.circle(color_image, p9, radius=3, color=color1, thickness=-1)
 
            cv2.line(color_image, p1, p2, color1, thickness=None, lineType=None, shift=None)
            cv2.line(color_image, p1, p3, color1, thickness=None, lineType=None, shift=None)
            cv2.line(color_image, p3, p4, color1, thickness=None, lineType=None, shift=None)
            cv2.line(color_image, p2, p4, color1, thickness=None, lineType=None, shift=None)
            cv2.line(color_image, p6, p7, color1, thickness=None, lineType=None, shift=None)
            cv2.line(color_image, p6, p8, color1, thickness=None, lineType=None, shift=None)
            cv2.line(color_image, p1, p6, color1, thickness=None, lineType=None, shift=None)
            cv2.line(color_image, p2, p7, color1, thickness=None, lineType=None, shift=None)
            cv2.line(color_image, p3, p8, color1, thickness=None, lineType=None, shift=None)
            cv2.line(color_image, p4, p9, color1, thickness=None, lineType=None, shift=None)
            cv2.line(color_image, p7, p9, color1, thickness=None, lineType=None, shift=None)
            cv2.line(color_image, p8, p9, color1, thickness=None, lineType=None, shift=None)
            
            cv2.circle(depth_image, p1, radius=3, color=color1, thickness=-1)
            cv2.circle(depth_image, p2, radius=3, color=color1, thickness=-1)
            cv2.circle(depth_image, p3, radius=3, color=color1, thickness=-1)
            cv2.circle(depth_image, p4, radius=3, color=color1, thickness=-1)
            cv2.circle(depth_image, p6, radius=3, color=color1, thickness=-1)
            cv2.circle(depth_image, p7, radius=3, color=color1, thickness=-1)
            cv2.circle(depth_image, p8, radius=3, color=color1, thickness=-1)
            cv2.circle(depth_image, p9, radius=3, color=color1, thickness=-1)

            cv2.line(depth_image, p1, p2, color1, thickness=None, lineType=None, shift=None)
            cv2.line(depth_image, p1, p3, color1, thickness=None, lineType=None, shift=None)
            cv2.line(depth_image, p3, p4, color1, thickness=None, lineType=None, shift=None)
            cv2.line(depth_image, p2, p4, color1, thickness=None, lineType=None, shift=None)
            cv2.line(depth_image, p6, p7, color1, thickness=None, lineType=None, shift=None)
            cv2.line(depth_image, p6, p8, color1, thickness=None, lineType=None, shift=None)
            cv2.line(depth_image, p1, p6, color1, thickness=None, lineType=None, shift=None)
            cv2.line(depth_image, p2, p7, color1, thickness=None, lineType=None, shift=None)
            cv2.line(depth_image, p3, p8, color1, thickness=None, lineType=None, shift=None)
            cv2.line(depth_image, p4, p9, color1, thickness=None, lineType=None, shift=None)
            cv2.line(depth_image, p7, p9, color1, thickness=None, lineType=None, shift=None)
            cv2.line(depth_image, p8, p9, color1, thickness=None, lineType=None, shift=None)
            # Apply colormap on depth image (image must be converted to 8-bit per pixel first)
            depth_colormap = cv2.applyColorMap(cv2.convertScaleAbs(depth_image, alpha=0.03), cv2.COLORMAP_JET)

            # marker처리
            marker_results = marker_model(color_image, verbose=False)
            detections, conf_old, det = [], 0, None
            # Extract marker detections
            for box in marker_results[0].boxes:
                x1, y1, x2, y2 = box.xyxy[0].tolist()
                conf = float(box.conf[0])
                cls = int(box.cls[0])
                if conf > 0.5:    # confidence output is rather low. so, lower the threshold
                    detections.append(("marker", conf, (x1, y1, x2, y2)))

            # Draw all detections
            for det in detections:
                label, conf, (x1, y1, x2, y2) = det
                cv2.rectangle(color_image, (int(x1), int(y1)), (int(x2), int(y2)), (0,255,0), 2) #BGR
                cv2.putText(color_image, f"{label} {conf:.2f}", (int(x1), int(y1)-10),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0,255,0), 2)

            # Tracking 
            deepsort_dets = []
            for _, conf, (x1, y1, x2, y2) in detections:
                w = x2 - x1
                h = y2 - y1
                deepsort_dets.append([[x1, y1, w, h], conf])
            
            tracks = tracker.update_tracks(deepsort_dets, frame=color_image)

            for t in tracks:
                if not t.is_confirmed():
                    print("t.is_confirmed")
                    continue
                x1, y1, x2, y2 = map(int, t.to_ltrb())
                track_id = t.track_id
                label = t.get_det_class()  # optional
                cv2.rectangle(color_image, (x1, y1), (x2, y2), (0,0,255), 2) #BGR
                cv2.putText(color_image, f"STATIONARY", (x1, y1-25),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0,0,255), 2)
                # cv2.putText(color_image, f"ID {track_id}{label}", (x1, y1-10),
                #             cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0,0,255), 2)

            # Robot Arm 움직임 판단

            # yolov8 처리
            results= model(color_image, stream=True, verbose=False)
            class_ids = []
            confidences = []
            bboxes = []
            for result in results:
                boxes = result.boxes
                for box in boxes:
                    confidence = box.conf
                    if confidence > 0.5:
                        xyxy = box.xyxy.tolist()[0]
                        bboxes.append(xyxy)
                        confidences.append(float(confidence))
                        class_ids.append(box.cls.tolist())

            result_boxes = cv2.dnn.NMSBoxes(bboxes, confidences, 0.25, 0.45, 0.5)

            violate = False # for each frame, start with assumption that there's no zone violation
            #current_time = datetime.now(timezone.utc)
            now_kst = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
            cv2.putText(color_image, f"{now_kst}", (10, 20), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255,255,255), 2)

            for i in range(len(bboxes)):
                label = str(CLASSES[int(class_ids[i][0])])

                #if label == 'person' and confidences[i]>0.8:
                #current_time = datetime.utcnow()
                #print(f"Inference at ==> {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                now_kst = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
                #log.info(f"[Inference] {now_kst} State={state}")


                if label == 'person':
                    if i in result_boxes:
                        bbox = list(map(int, bboxes[i])) 
                        x1, y1, x2, y2 = bbox
                        # cx = int((x1+x2)/2)    
                        # cy = int((y1+y2)/2.3)
                        
                        ###################################
                        # Media pipe handling
                        ###################################
                        cropped = color_image[y1:y2, x1:x2]
                        if cropped.size == 0:
                            continue

                        # Convert to RGB for MediaPipe
                        #cropped_rgb = cv2.cvtColor(cropped, cv2.COLOR_BGR2RGB)
                        results_mp = pose.process(cropped)     # Pose detection

                        # Collect keys tha
                        key_list = []
                        if results_mp.pose_landmarks:

                            # Get only visible keypoints
                            visible_landmarks = [                     
                                (idx, lm)
                                for idx, lm in enumerate(results_mp.pose_landmarks.landmark)
                                if lm.visibility > VISIBILITY_THRESHOLD
                            ]
  
                            for idx, lm in visible_landmarks:
                                if idx in my_pose:
                                    #print(f"{my_pose[idx]}: x={lm.x}, y={lm.y}, z={lm.z}")
                                    cx = int(lm.x * (x2 - x1)) + x1
                                    if cx >= TARGET_WIDTH: cx = TARGET_WIDTH-1
                                    cy = int(lm.y * (y2 - y1)) + y1
                                    if cy >= TAREGET_HEIGHT: cy=TAREGET_HEIGHT-1
                                    #print(f"cx={cx}, cy={cy}")
                                    cv2.circle(color_image, (cx, cy), 3, (0, 255, 0), -1)

                                    key_list.append([cx,cy, depth_image[cy, cx]])

                        # Draw person box
                        cv2.rectangle(color_image, (x1, y1), (x2, y2), (255, 0, 0), 2)

                        key_cnt, key_str, state = 0, "", False   
                        for key in key_list:
                            cz = key[2]
                            if  a1*cx + b1*cy + c1*cz + d1 > 0:
                                if  a2*cx + b2*cy + c2*cz + d2 < 0:
                                    if  a3*cx + b3*cy + c3*cz + d3 > 0:
                                        if  a4*cx + b4*cy + c4*cz + d4 < 0:
                                            key_cnt+=1
                                            #print(f"Person{cx},{cy}:{state} In")
                                        else:
                                            key_str+="4"
                                    else:
                                        key_str+="3"
                                else:
                                    key_str+="2"
                            else:
                                key_str+="1"
                        
                        #print(key_str)

                        # For one person, if half of keypoints are 
                        if key_cnt > len(key_list) // 2:
                            print(f"[{key_cnt}/{len(key_list)}] VIOLATION")
                            violate = True
                        else:
                            #print(f"[{key_cnt}] _______________________________________")
                            pass
                    
                        #label = label + " " + str(format(confidences[i],'.2f')) + " " + str(depth) + "cm"
                        color = colors[i]
                        color = (int(color[0]), int(color[1]), int(color[2]))
                        
                        tl = 2 # line/font thickness
                        tf = max(tl - 1, 1)  # font thickness
                        t_size = cv2.getTextSize(label, 0, fontScale=tl / 3, thickness=tf)[0]
                        aboveheadbox = x1 + t_size[0], y1 - t_size[1] - 3

                        cv2.rectangle(color_image, (x1, y1), (x2, y2), color, 2)# color rectangle
                        cv2.rectangle(color_image, (x1, y1), aboveheadbox, color, -1, cv2.LINE_AA)# label rectangle
                        #cv2.circle(color_image, (cx, cy), 5, color, -1)# Center Point
                        cv2.putText(color_image, label, (x1, y1 - 2), 0, tl/3, [255, 255, 255], thickness=tf, lineType=cv2.LINE_AA)  # label
                        
                        cv2.rectangle(depth_colormap, (x1, y1), (x2, y2), [0, 0, 0], 2)# depth rectangle
                        cv2.rectangle(depth_colormap, (x1, y1), aboveheadbox, [0, 0, 0], -1, cv2.LINE_AA)# label rectangle
                        #cv2.circle(depth_colormap, (cx, cy), 5, [0, 0, 0], -1)# Center Point
                        cv2.putText(depth_colormap, label, (x1, y1 - 2), 0, tl/3, [255, 255, 255], thickness=tf, lineType=cv2.LINE_AA)# label

            depth_colormap_dim = depth_colormap.shape
            color_colormap_dim = color_image.shape
            
            # If depth and color resolutions are different, resize color image to match depth image for display
            if depth_colormap_dim != color_colormap_dim:
                resized_color_image = cv2.resize(color_image, dsize=(depth_colormap_dim[1], depth_colormap_dim[0]), interpolation=cv2.INTER_AREA)
                images = np.hstack((resized_color_image, depth_colormap))
            else:
                images = np.hstack((color_image, depth_colormap))
            
            # Show images
            cv2.namedWindow('RealSense', cv2.WINDOW_AUTOSIZE)
            #cv2.imshow('colorImage', color_image)
            cv2.imshow('RealSense', images)
            #imageio.imwrite("./rs_streamdata/rgb.jpg", color_image)


            #inserting shared memory
            with lock:
                #shared_a = np.ndarray(color_image.shape, dtype=color_image.dtype, buffer=stream_shm)
                shared_a[:] = color_image
                #log.info(f"[s_3dzone_thread] → buffer size, stream_shm:{stream_shm.shape}")

            # Schedule GLib loop to stop after work is done
            GLib.idle_add(streaming_loop.quit)
            cv2.waitKey(1)

            sys_time = float(time.time())-32400 
            #log.info(f"[Inference] : delta time:{abs(sys_time - edge_time)}")
            if abs(sys_time - edge_time) > 0.5: # Lagging시간이 심하면, 추론 따윈 건너뛰기
                producer.send(TOPIC_PUBLISH, {
                        "timestamp": now_kst,
                        "state": violate
                })
                edge_time = sys_time

            ###################################################
            # Violation rule setting and writing video file 
            ##################################################
            frame_buffer.append(color_image)
            if violate is True:
                violate_cnt += 1
                if violate_cnt > MAX_VIOLATE:
                    if in_writing !=True:
                        for frame in frame_buffer:
                            print("Frame Buffer Writing...")
                            out.write(frame)
                    out.write(color_image)
                    in_writing=True
                    no_violate_cnt =0
                    print(f"{now_kst} Write video recording")
                else:
                    if in_writing is True:
                        out.release()
                        in_writing=False
                        print("Stop video recording")
                    else:
                        pass
            else:
                no_violate_cnt += 1
                if no_violate_cnt > MAX_NONVIOLATE and in_writing == True:
                    out.release()
                    in_writing=False
                    violate_cnt = 0
                    print("Stop video recording") 
   
        
    except Exception as e:
        print("[s_3dzone_thread] Exception:", e)
        pipeline.stop()
        pose.close()

#################################################################################################
# gstreamer thread:
# 
################################################################################################
# TARGET_WIDTH = 640
# TAREGET_HEIGHT= 480
# TARGET_DEPTH = 3
# cap_shape=(TAREGET_HEIGHT, TARGET_WIDTH, TARGET_DEPTH)
# existing_shm = shared_memory.SharedMemory(name='shm')

class CustomMediaFactory(GstRtspServer.RTSPMediaFactory):
    def __init__(self, **properties):
        super(CustomMediaFactory, self).__init__(**properties)
        self.number_frames = 0
        self.fps = 30
        self.duration = 1 / self.fps * Gst.SECOND  # duration per frame
        self.launch_string = (
            "appsrc name=mysrc is-live=true block=true format=TIME caps=video/x-raw,format=BGR,width=640,height=480,framerate=30/1 ! "
            "videoconvert ! "
            "video/x-raw,format=I420 ! " #"edgetv ! "
            #"x264enc tune=zerolatency bitrate=512 speed-preset=ultrafast ! "
            "x264enc speed-preset=ultrafast tune=zerolatency ! "
            "rtph264pay config-interval=1 name=pay0 pt=96"
        )

    def do_create_element(self, url):
        print("[RTSP] Creating pipeline element")
        return Gst.parse_launch(self.launch_string)

    def do_configure(self, rtsp_media):
        print("[RTSP] Configuring media")
        self.pipeline = rtsp_media.get_element()
        self.appsrc = self.pipeline.get_child_by_name("mysrc")

        self.appsrc.connect("need-data", self.on_need_data)
        rtsp_media.connect("new-state", self.on_media_state_changed)
        # Detect client disconnect
        rtsp_media.connect("unprepared", self.on_client_disconnected)

    def on_client_disconnected(self, media):
        print("[RTSP] Client disconnected, cleaning up.")
        self.cleanup()

    def cleanup(self):
        self.appsrc = None
        self.number_frames = 0

    def on_media_state_changed(self, media, state):
        print(f"[RTSP] Media state changed to: {Gst.Element.state_get_name(state)}")

        if state == Gst.State.READY:
            print("[RTSP] Client requested setup (READY)")
        elif state == Gst.State.PLAYING:
            print("[RTSP] Client started playback (PLAYING)")
            self.running = True
        elif state == Gst.State.NULL:
            print("[RTSP] Media state reset: client disconnected or pipeline stopped")
            self.frame_count = 0
        else:
            print("[RTSP] Client stopped or disconnected")
            self.running = False

    def on_need_data(self, src, length):
        # Read frame from camera or file or synthetic
        #ret, frame = self.cap.read()
        with lock:
            #self.shared_a = np.frombuffer(stream_shm, dtype=np.uint8)
            c = np.ndarray(cap_shape, dtype=np.uint8, buffer=shared_a)
        frame = c
        ret = frame is not None
        if not ret:
            print("Failed to read frame")
            return

        frame = cv2.resize(frame, (640, 480))
        data = frame.tobytes()

        buf = Gst.Buffer.new_allocate(None, len(data), None)
        buf.fill(0, data)
        buf.duration = self.duration
        buf.pts = buf.dts = int(self.number_frames * self.duration)
        self.number_frames += 1

        retval = self.appsrc.emit("push-buffer", buf)

        if retval == Gst.FlowReturn.FLUSHING:
            print("[Warning] Pipeline is flushing. Stopping data push.")
            return
        elif retval != Gst.FlowReturn.OK:
            print(f"[Error] Push buffer failed: {retval}")

class GstServer():
    def __init__(self):
        self.rtspServer = GstRtspServer.RTSPServer()
        self.rtspServer.set_service("8554")   # RTSP port
        self.rtspServer.connect("client-connected", self.client_connected) # + client connection 

    def client_connected(self, arg1, arg2):
        print('Client connected..')


def on_media_state_changed(media, state):
    print(f"[RTSP] Media state changed to: {Gst.Element.state_get_name(state)}")

def on_media_unprepared(media):
    print("[RTSP] Media unprepared (client disconnected or pipeline stopped)")# Reset your application state here

def on_media_constructed(factory, media):
    print("[RTSP] Media constructed")
    media.connect("new-state", on_media_state_changed) # Listen for state changes
    media.connect("unprepared", on_media_unprepared) # Listen for disconnection or teardown

def on_client_closed(client):
    print("[RTSP] Client disconnected")# Optionally reset custom flags or counters
    

def client_connected(server, client):
    print("[RTSP] New client connected")
    client.connect("closed", on_client_closed)# Connect to session-removed to detect disconnections

def gstreaming_thread():
    print("[GLib] MainLoop starting")
    try:
        streaming_loop.run()
    except Exception as e:
        print("[GLib] Exception:", e)
    print("[GLib] MainLoop exited")



if __name__=="__main__":
    log.info("메인 스레드 시작 — Consumer & Inference 스레드 구동")

    factory = CustomMediaFactory()
    factory.set_shared(True)
    factory.connect("media-constructed", on_media_constructed)
    rtspServer = GstServer()
    mount_points = rtspServer.rtspServer.get_mount_points()
    mount_points.add_factory("/test", factory)   # RTSP URL sub string
    rtspServer.rtspServer.connect("client-connected", client_connected)
    rtspServer.rtspServer.attach(None)

    print("RTSP stream ready at rtsp://141.223.106.239:10004/test")
    time.sleep(1)
    streaming_loop = GLib.MainLoop()
    t1 = threading.Thread(target=safe3dzone_thread, daemon=True)
    t2 = threading.Thread(target=gstreaming_thread, daemon=True)
    t3 = threading.Thread(target=keyboard_thread, daemon=True)

    t1.start()
    time.sleep(3)
    t2.start()
    t3.start()

    t1.join()
    t2.join()
