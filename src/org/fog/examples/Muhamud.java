package org.fog.examples;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.network.datacenter.NetworkHost;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.sdn.overbooking.BwProvisionerOverbooking;
import org.cloudbus.cloudsim.sdn.overbooking.PeProvisionerOverbooking;
import org.fog.application.AppEdge;
import org.fog.application.AppLoop;
import org.fog.application.Application;
import org.fog.application.selectivity.FractionalSelectivity;
import org.fog.entities.Actuator;
import org.fog.entities.EndDevice;
import org.fog.entities.FogBroker;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.entities.Sensor;
import org.fog.entities.Tuple;
import org.fog.network.EdgeSwitch;
import org.fog.network.PhysicalTopology;
import org.fog.network.Switch;
import org.fog.placement.ModulePlacementPolicy_MohitTaneja;
import org.fog.policy.AppModuleAllocationPolicy;
import org.fog.scheduler.AppModuleScheduler;
import org.fog.utils.FogLinearPowerModel;
import org.fog.utils.FogUtils;
import org.fog.utils.Logger;
import org.fog.utils.TimeKeeper;
import org.fog.utils.distribution.DeterministicDistribution;


public class Muhamud {
	
	//Global Lists for Physical components
	static List<FogDevice> fogDevices = new ArrayList<FogDevice>();
	static List<Sensor> sensors = new ArrayList<Sensor>();
	static List<Actuator> actuators = new ArrayList<Actuator>();
	static List<Application> applications = new ArrayList<Application>();
	
	public static void main(String[] args) {
		
		Logger.ENABLED = false;
		Logger.enableTag("FOG_DEVICE");
		Logger.enableTag("SWITCH");
		Logger.enableTag("LINK");
		
		try {
			
		//-----------------------Initialization step 1 -----------------------------------------	
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false; // mean trace events
			CloudSim.init(num_user, calendar, trace_flag);
			FogBroker broker = new FogBroker("Broker");
			
		//-----------------------Create the Physical Topology Step 3 ----------------------------
			createPhysicalTopology(broker.getId(), broker);
		
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unexpected Error");
		}
		
	}
	
	
	private static void createApplicationandEndDeviceandSubmit(String appId, int userId, String module, String sensorName, String actuatorName, int swId, FogBroker broker, String enddevicename, int enddevicelatency) {
			int transmissionInterval = 5000;
			Application application = Application.createApplication(appId, userId);
			application.addAppModule(module, 10, 10, 1);
			
			application.addAppEdge(sensorName, module, 30000, 10*1024, sensorName, Tuple.UP, AppEdge.SENSOR);
			application.addAppEdge(module, actuatorName, 30000, 10*1024, actuatorName, Tuple.DOWN, AppEdge.ACTUATOR);  // adding edge from Client module to Display (actuator) carrying tuples of type SELF_STATE_UPDATE
			
			application.addTupleMapping(module, sensorName, actuatorName, new FractionalSelectivity(1.0)); 
			
			final AppLoop loop1 = new AppLoop(new ArrayList<String>(){{add(sensorName);add(module);add(actuatorName);}});
			System.out.println("LOOP ID at creation = "+loop1.getLoopId());
			List<AppLoop> loops = new ArrayList<AppLoop>(){{add(loop1);}};
			application.setLoops(loops);
			
			application.setUserId(userId);
			applications.add(application);
			EndDevice dev = new EndDevice(enddevicename);
			
			Sensor sensor = new Sensor(sensorName, sensorName, userId, appId, new DeterministicDistribution(transmissionInterval), application);
			Actuator actuator = new Actuator(actuatorName, userId, appId, actuatorName, application);
			dev.addSensor(sensor);
			dev.addActuator(actuator);
			sensors.add(sensor);
			actuators.add(actuator);
			PhysicalTopology.getInstance().addEndDevice(dev);
			PhysicalTopology.getInstance().addLink(dev.getId(), swId, enddevicelatency, 1000);
			
			broker.submitApplication(application, 0, 
					new ModulePlacementPolicy_MohitTaneja(fogDevices, sensors, actuators, application));
			
	}
	
	private static void createPhysicalTopology(int userId, FogBroker broker) {
		//-----------------------create Physical components-----------------------------------
			//---------HL
			FogDevice fd00 = createFogDevice("FD00", true, 102400, 100000, 10000);
			Switch sw00 = new Switch("SW00",geoHash(90,90,7));
			//---------ML
			FogDevice fd10 = createFogDevice("FD10", false, 10240, 10000, 1000);
		    FogDevice fd11 = createFogDevice("FD11", false, 10240, 10000, 1000);
			FogDevice fd12 = createFogDevice("FD12", false, 10240, 10000, 1000);
			FogDevice fd13 = createFogDevice("FD13", false, 10240, 10000, 1000);
			FogDevice fd14 = createFogDevice("FD14", false, 10240, 10000, 1000);
			FogDevice fd15 = createFogDevice("FD15", false, 10240, 10000, 1000);
			Switch sw10 = new Switch("SW10",geoHash(-30,0,7));
			Switch sw11 = new Switch("SW11",geoHash(30,0,7));
			Switch sw14 = new Switch("SW14",geoHash(0,0,7));

			//---------LL
			FogDevice fd20 = createFogDevice("FD20", false, 10240, 10000, 1000);
			FogDevice fd21 = createFogDevice("FD21", false, 10240, 10000, 1000);
			FogDevice fd22 = createFogDevice("FD22", false, 10240, 10000, 1000);
			Switch sw20 = new Switch("SW20",geoHash(-30,-30,7));
			Switch sw21 = new Switch("SW21",geoHash(30,30,7));
			Switch sw24 = new Switch("SW24",geoHash(45,45,7));

			int[] switchids = {sw20.getId(),sw21.getId(),sw24.getId()};
		
		//-----------------------add to global Lists------------------------------------------
			fogDevices.add(fd00);
			fogDevices.add(fd10);
			fogDevices.add(fd11);
			fogDevices.add(fd12);
			fogDevices.add(fd13);
			fogDevices.add(fd14);
			fogDevices.add(fd15);
			fogDevices.add(fd20);
			fogDevices.add(fd21);
			fogDevices.add(fd22);
		//-----------------------add links and connections------------------------------------
			PhysicalTopology.getInstance().addFogDevice(fd00);
			PhysicalTopology.getInstance().addFogDevice(fd10);
			PhysicalTopology.getInstance().addFogDevice(fd11);
			PhysicalTopology.getInstance().addFogDevice(fd12);
			PhysicalTopology.getInstance().addFogDevice(fd13);
			PhysicalTopology.getInstance().addFogDevice(fd14);
			PhysicalTopology.getInstance().addFogDevice(fd15);
			PhysicalTopology.getInstance().addFogDevice(fd20);
			PhysicalTopology.getInstance().addFogDevice(fd21);
			PhysicalTopology.getInstance().addFogDevice(fd22);
			PhysicalTopology.getInstance().addSwitch(sw00);
			PhysicalTopology.getInstance().addSwitch(sw10);
			PhysicalTopology.getInstance().addSwitch(sw11);
			PhysicalTopology.getInstance().addSwitch(sw14);
			PhysicalTopology.getInstance().addSwitch(sw20);
			PhysicalTopology.getInstance().addSwitch(sw21);
			PhysicalTopology.getInstance().addSwitch(sw24);
			
			//------------LL
			PhysicalTopology.getInstance().addLink(sw20.getId(), fd20.getId(), 1, 1000);
			PhysicalTopology.getInstance().addLink(sw24.getId(), fd22.getId(), 1, 1000);
			PhysicalTopology.getInstance().addLink(sw21.getId(), fd21.getId(), 1, 1000);
			//------------ML
			PhysicalTopology.getInstance().addLink(sw10.getId(), fd10.getId(), 1, 1000);
			PhysicalTopology.getInstance().addLink(sw10.getId(), fd11.getId(), 1, 1000);
			PhysicalTopology.getInstance().addLink(sw14.getId(), fd14.getId(), 1, 1000);
			PhysicalTopology.getInstance().addLink(sw14.getId(), fd15.getId(), 1, 1000);
			PhysicalTopology.getInstance().addLink(sw11.getId(), fd12.getId(), 1, 1000);
			PhysicalTopology.getInstance().addLink(sw11.getId(), fd13.getId(), 1, 1000);
			//------------HL
			PhysicalTopology.getInstance().addLink(sw00.getId(), fd00.getId(), 1000, 1000);
			//Latency Switches
			//------------ML
			PhysicalTopology.getInstance().addLink(sw20.getId(), sw10.getId(), 50, 1000);
			PhysicalTopology.getInstance().addLink(sw24.getId(), sw14.getId(), 50, 1000);
			PhysicalTopology.getInstance().addLink(sw21.getId(), sw11.getId(), 50, 1000);
			PhysicalTopology.getInstance().addLink(sw10.getId(), sw00.getId(), 200, 1000);
			PhysicalTopology.getInstance().addLink(sw14.getId(), sw00.getId(), 200, 1000);
			PhysicalTopology.getInstance().addLink(sw11.getId(), sw00.getId(), 200, 1000);
			//----------------Create Application End Device Creation-----------------------------
			
			int count = 100;
			int max = 90;
			int min = -30;
			
			Switch[] Switches= {sw00,sw10,sw14,sw11,sw20,sw21,sw24};
			//int loop = 1;
			//for(int j=0;j<loop;j++) {
				for(int i=0;i<count;i++) {
					int lat = new Random().nextInt(max - min) + min;
					int lon = new Random().nextInt(max - min) + min;
					String devgeomap = geoHash(lat, lon, 7);
					String appid = "_app".concat(Integer.toString(i));
					String modulename = "MODULE".concat(Integer.toString(i)) ;
					String sensorname = "IoT_Sensor".concat(Integer.toString(i));
					String actuatorname = "Display".concat(Integer.toString(i));
					String enddevicename = "DEV-".concat(Integer.toString(i));
					
					//Random Assignment
					//int switchselected = getRandom(switchids);
					//int enddevicelatency = 50;
					//if(i<20 && switchselected == sw20.getId()) {
					//	enddevicelatency = 1;
					//}else if(i>19 && i>40 && switchselected == sw21.getId()) {
					//	enddevicelatency = 1;
					//}else if(i>39 && switchselected == sw24.getId()){
					//	enddevicelatency = 1;
					//}

					
					int enddevicelatency = 50;
					//Location based assignment
					int diff = 10;
					Switch switchselected = sw00;
					for(Switch switch1 : Switches){
						int diff1 = devgeomap.compareTo(switch1.getGeomap());
						if(diff1 < diff) {
							diff = diff1;
							switchselected = switch1;
							enddevicelatency = 1;
						}
					}
					System.out.println("==========================================================================================================");
					System.out.println(enddevicename +" is on ("+lat+","+lon+")");
					System.out.println(enddevicename +" is on Switch ID " + switchselected.getName() + " with enddevicelatency " + enddevicelatency);
					System.out.println("==========================================================================================================");
					createApplicationandEndDeviceandSubmit(appid, userId, modulename, sensorname,actuatorname, switchselected.getId(), broker, enddevicename, enddevicelatency);
				}
				
				//-----------------------validation---------------------------------------------------
				if (PhysicalTopology.getInstance().validateTopology()) {
					System.out.println("Topology validation successful");
					PhysicalTopology.getInstance().setUpEntities();
				} else {
					System.out.println("Topology validation UNsuccessful");
					System.exit(1);
				}
				
				//-----------------------Set the Physical components to Broker Step 4 -------------------
				broker.setFogDeviceIds(getIds(fogDevices));
				broker.setSensorIds(getIds(sensors));
				broker.setActuatorIds(getIds(actuators));
				System.out.println("-------------Initiation Completed---------------------");
				//-----------------------
				TimeKeeper.getInstance().setSimulationStartTime(Calendar.getInstance().getTimeInMillis());
				//-----------------------
				CloudSim.startSimulation();
				CloudSim.stopSimulation();
			}
	//}
	
	private static String geoHash(double latitude, double longitude, int precision) {
		/**
		Encode a point into a geohash string.

	    Args:
	        latitude (float): The latitude of the point in degrees.
	        longitude (float): The longitude of the point in degrees.
	        precision (int): The desired length of the geohash string.

	    Returns:
	        str: The geohash string of the point.
		***/
		
		// Define the geohash alphabet
		int[] _base32 = {'0','1','2','3','4','5','6','7','8','9','b','c','d','e','f','g','h','j','k','m','n','p','q','r','s','t','u','v','w','x','y','z'};

		// Define the ranges of latitude and longitude
	    double[] lat_range = {-90.0, 90.0};
	    double[] lon_range = {-180.0, 180.0};
		
	    // Initialize variables
	    String geohash = "";
	    int bits = 0;
	    int bits_total = 0;
	    int mid;

	    while(geohash.length() < precision){
	    	//Check if we need to encode the longitude or the latitude
	        if(bits_total % 2 == 0) {
	            mid = (int) ((lon_range[0] + lon_range[1]) / 2);
	            if(longitude > mid) {
	                bits |= 1 << (4 - bits_total % 5);
	                lon_range[0] = mid;
	                lon_range[1] = lon_range[1];
	            }else {
	            	lon_range[0] = lon_range[0];
	            	lon_range[1] = mid;
	            }    
	        }else {
	        	mid = (int) ((lat_range[0] + lat_range[1]) / 2);
	            if(latitude > mid){
	            	bits |= 1 << (4 - bits_total % 5);
	            	lat_range[0] = mid;
	                lat_range[1] = lon_range[1];
	            }else{
	            	lat_range[0] = lon_range[0];
	            	lat_range[1] = mid;       	
	            }
	        }
	        
	        bits_total += 1;
	        
	        // Add a character to the geohash string every 5 bits
	        if(bits_total % 5 == 0) {
	            geohash += _base32[bits];
	            bits = 0;
	        }
	    }
	   
		return geohash;
	}
	
	private static FogDevice createFogDevice(String nodeName, boolean isCloud, long mips, int ram, int bw) {
		
		List<Pe> peList = new ArrayList<Pe>();
		peList.add(new Pe(0, new PeProvisionerOverbooking(mips)));
		
		int hostId = FogUtils.generateEntityId();
		long storage = 10000000; // host storage
		double busyPower = 0.01;
		double idlePower = 0.01;
		
		PowerHost host = new PowerHost(
				hostId,
				new RamProvisionerSimple(ram),
				new BwProvisionerOverbooking(bw),
				storage,
				peList,
				new AppModuleScheduler(peList),
				new FogLinearPowerModel(busyPower, idlePower)
			);

		List<Host> hostList = new ArrayList<Host>();
		hostList.add(host);
		
		//-----------------NON CHANGING-----------------------------------
		String arch = "x86"; // system architecture
		String os = "Linux"; // operating system
		String vmm = "Xen";
		double time_zone = 10.0; // time zone this resource located
		double ratePerMips = 0.01;
		double cost = 3.0; // the cost of using processing in this resource
		double costPerMem = 0.05; // the cost of using memory in this resource
		double costPerStorage = 0.001; // the cost of using storage in this resource
		double costPerBw = 0.0; // the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>();
		//-----------------------------------------------------------------
		
		FogDeviceCharacteristics characteristics = new FogDeviceCharacteristics(isCloud,
				arch, os, vmm, host, time_zone, cost, costPerMem,
				costPerStorage, costPerBw);
		FogDevice fogdevice = null;
		try {
			// TODO Check about scheduling interval
			fogdevice = new FogDevice(nodeName, characteristics, 
					new AppModuleAllocationPolicy(hostList), storageList, 10, ratePerMips);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fogdevice;
	}
	
	public static List<Integer> getIds(List<? extends SimEntity> entities) {
		List<Integer> ids = new ArrayList<Integer>();
		for (SimEntity entity : entities) {
			ids.add(entity.getId());
		}
		return ids;
	}
	
	public static int getRandom(int[] array) {
	    int rnd = new Random().nextInt(array.length);
	    return array[rnd];
	}
}