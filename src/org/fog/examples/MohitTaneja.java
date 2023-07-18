package org.fog.examples;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

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


public class MohitTaneja {
	
	//Global Lists for Physical components
	static List<FogDevice> fogDevices = new ArrayList<FogDevice>();
	static List<Sensor> sensors = new ArrayList<Sensor>();
	static List<Actuator> actuators = new ArrayList<Actuator>();
	
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
		//-----------------------Application Creation Step 2 -----------------------------------
			String appId1 = "_app1"; // identifier of the application
			Application application1 = createApplication(appId1, broker.getId());
			application1.setUserId(broker.getId());
		//-----------------------Create the Physical Topology Step 3 ----------------------------
			createPhysicalTopology(broker.getId(), appId1, application1);
		//-----------------------Set the Physical components to Broker Step 4 -------------------
			broker.setFogDeviceIds(getIds(fogDevices));
			broker.setSensorIds(getIds(sensors));
			broker.setActuatorIds(getIds(actuators));
			System.out.println("-------------Initiation Completed---------------------");
		//-----------------------
	//		broker.submitApplication(application1, 0, 
	//				new ModulePlacementPolicy_MohitTaneja(fogDevices, sensors, actuators, application1));
			TimeKeeper.getInstance().setSimulationStartTime(Calendar.getInstance().getTimeInMillis());
		//-----------------------
			CloudSim.startSimulation();
			CloudSim.stopSimulation();
			
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unexpected Error");
		}
		
	}
	
	
	private static Application createApplication(String appId, int userId) {
		Application application = Application.createApplication(appId, userId);

		
		application.addAppModule("module_1", 500,1024,250);
		application.addAppModule("module_2", 1000,4096,500);
		application.addAppModule("module_3", 2000,2048,1000);
		application.addAppModule("module_4", 1500,1024,300);
		application.addAppModule("module_5", 3000,6144,2000);
		application.addAppModule("module_6", 1500,8192,5000);
		
		application.addAppEdge("IoT_Sensor", "module_1", 30000, 10*1024, "IoT_Sensor", Tuple.UP, AppEdge.SENSOR);
		application.addAppEdge("module_1", "Display", 30000, 10*1024, "ACTUATOR_A", Tuple.UP, AppEdge.ACTUATOR);
		application.addAppEdge("module_1", "Display", 30000, 10*1024, "ACTUATOR_A", Tuple.UP, AppEdge.ACTUATOR);
		application.addAppEdge("module_1", "module_2", 30000, 10*1024, "TT_2", Tuple.UP, AppEdge.MODULE);
		application.addAppEdge("module_2", "module_1", 30000, 10*1024, "TT_9", Tuple.UP, AppEdge.MODULE);
		application.addAppEdge("module_2", "module_3", 30000, 10*1024, "TT_3", Tuple.UP, AppEdge.MODULE);
		application.addAppEdge("module_3", "module_2", 30000, 10*1024, "TT_8", Tuple.UP, AppEdge.MODULE);
		application.addAppEdge("module_3", "module_4", 30000, 10*1024, "TT_4", Tuple.UP, AppEdge.MODULE);
		application.addAppEdge("module_4", "module_3", 30000, 10*1024, "TT_7", Tuple.UP, AppEdge.MODULE);
		application.addAppEdge("module_4", "module_5", 30000, 10*1024, "TT_5", Tuple.UP, AppEdge.MODULE);
		application.addAppEdge("module_5", "module_4", 30000, 10*1024, "TT_6", Tuple.UP, AppEdge.MODULE);
		application.addAppEdge("module_5", "module_6", 30000, 10*1024, "TT_10", Tuple.UP, AppEdge.MODULE);
		application.addAppEdge("module_6", "module_1", 30000, 10*1024, "TT_11", Tuple.UP, AppEdge.MODULE);
		
		application.addTupleMapping("module_1", "IoT_Sensor", "TT_2", new FractionalSelectivity(1.0)); 
		application.addTupleMapping("module_1", "TT_9", "ACTUATOR_A", new FractionalSelectivity(1.0)); 
		application.addTupleMapping("module_1", "TT_11", "ACTUATOR_B", new FractionalSelectivity(1.0)); 
		application.addTupleMapping("module_2", "TT_2", "TT_3", new FractionalSelectivity(1.0)); 
		application.addTupleMapping("module_2", "TT_8", "TT_9", new FractionalSelectivity(1.0)); 
		application.addTupleMapping("module_3", "TT_3", "TT_4", new FractionalSelectivity(1.0)); 
		application.addTupleMapping("module_3", "TT_7", "TT_8", new FractionalSelectivity(1.0)); 
		application.addTupleMapping("module_4", "TT_4", "TT_5", new FractionalSelectivity(1.0)); 
		application.addTupleMapping("module_4", "TT_6", "TT_7", new FractionalSelectivity(1.0));
		application.addTupleMapping("module_5", "TT_5", "TT_6", new FractionalSelectivity(1.0));
		application.addTupleMapping("module_6", "TT_10", "TT_11", new FractionalSelectivity(1.0));
		
		final AppLoop loop1 = new AppLoop(new ArrayList<String>(){{add("IoT_Sensor");add("module_1");add("module_2");add("module_3");add("module_4");add("module_5");add("module_4");add("module_3");add("module_2");add("module_1");add("Display");}});
		final AppLoop loop2 = new AppLoop(new ArrayList<String>(){{add("module_5");add("module_6");add("module_1");add("Display");}});
		//System.out.println("LOOP ID at creation = "+loop1.getLoopId());
		List<AppLoop> loops = new ArrayList<AppLoop>(){{add(loop1);add(loop2);}};
		application.setLoops(loops);
		
		return application;
	}
	
	private static void createPhysicalTopology(int userId, String appId1, Application application1) {
		//-----------------------create Physical components-----------------------------------
			FogDevice fd3 = createFogDevice("FD3", true, 102400, 4000, 10000);
			FogDevice fd2 = createFogDevice("FD2", true, 102400, 4000, 10000);
			FogDevice fd1 = createFogDevice("FD1", true, 102400, 4000, 10000);
			
			Switch sw1 = new Switch("SW1","geomap");
			Switch sw2 = new Switch("SW2","geomap");
			
			EndDevice dev1 = new EndDevice("DEV1");
			
			int transmissionInterval = 5000;
			Sensor sensor1 = new Sensor("IoT_Sensor", "IoT_Sensor", userId, appId1, new DeterministicDistribution(transmissionInterval), application1);
			Actuator actuator1 = new Actuator("Display", userId, appId1, "ACTUATOR_A", application1);
			dev1.addSensor(sensor1);
			dev1.addActuator(actuator1);
			
		//-----------------------add to global Lists------------------------------------------
			sensors.add(sensor1);
			actuators.add(actuator1);
			fogDevices.add(fd1);
			fogDevices.add(fd2);
			fogDevices.add(fd3);
		//-----------------------add links and connections------------------------------------
			PhysicalTopology.getInstance().addFogDevice(fd1);
			PhysicalTopology.getInstance().addFogDevice(fd2);
			PhysicalTopology.getInstance().addFogDevice(fd3);
			PhysicalTopology.getInstance().addSwitch(sw1);
			PhysicalTopology.getInstance().addSwitch(sw2);
			PhysicalTopology.getInstance().addEndDevice(dev1);
			
			PhysicalTopology.getInstance().addLink(dev1.getId(), sw1.getId(), 10, 1000);
			PhysicalTopology.getInstance().addLink(sw1.getId(), fd1.getId(), 10, 1000);
			PhysicalTopology.getInstance().addLink(sw1.getId(), fd2.getId(), 10, 1000);
			PhysicalTopology.getInstance().addLink(sw1.getId(), sw2.getId(), 10, 1000);
			PhysicalTopology.getInstance().addLink(sw2.getId(), fd3.getId(), 10, 1000);
		//-----------------------validation---------------------------------------------------
			if (PhysicalTopology.getInstance().validateTopology()) {
				System.out.println("Topology validation successful");
				PhysicalTopology.getInstance().setUpEntities();
				
			} else {
				System.out.println("Topology validation UNsuccessful");
				System.exit(1);
			}
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
}