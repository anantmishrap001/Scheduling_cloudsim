package cloudsim.simulations;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;

import java.util.*;

public class AWS_Lambda_Simulation {

    private static final double WARM_START_DURATION = 300.0;

    public static void main(String[] args) {
        try {
            int numUsers = 1;
            Calendar calendar = Calendar.getInstance();
            boolean traceFlag = false;

            CloudSim.init(numUsers, calendar, traceFlag);

            Datacenter datacenter = createDatacenter("Datacenter_0");
            DatacenterBroker broker = createBroker();
            int brokerId = broker.getId();

            List<CloudletExecutionInfo> executionSchedule = createExecutionSchedule(brokerId);
            List<Vm> vmList = new ArrayList<>();
            Map<Integer, List<MemoryUsageRecord>> vmMemoryUsage = new HashMap<>();

            int vmId = 0;
            for (int i = 0; i < 5; i++) {
                Vm vm = new Vm(vmId, brokerId, 1000, 1, 512, 1000, 10000, "Xen", new CloudletSchedulerTimeShared());
                vmList.add(vm);
                vmMemoryUsage.put(vmId, new ArrayList<>());
                vmId++;
            }
            broker.submitVmList(vmList);

            Map<Integer, Map<Integer, Double>> warmFunctions = new HashMap<>();

            executionSchedule.sort(Comparator.comparingDouble(CloudletExecutionInfo::getExecutionTime));

            for (CloudletExecutionInfo info : executionSchedule) {
                double currentTime = info.getExecutionTime();
                int functionType = info.getFunctionType();

                Vm selectedVm = vmList.get(functionType % vmList.size());
                double memoryUsage = 128;

                boolean isWarm = warmFunctions.containsKey(functionType)
                        && warmFunctions.get(functionType).containsKey(selectedVm.getId())
                        && currentTime - warmFunctions.get(functionType).get(selectedVm.getId()) <= WARM_START_DURATION;

                info.setVmId(selectedVm.getId());
                info.setWarmStart(isWarm);

                warmFunctions.computeIfAbsent(functionType, k -> new HashMap<>()).put(selectedVm.getId(), currentTime);

                vmMemoryUsage.get(selectedVm.getId()).add(new MemoryUsageRecord(currentTime, memoryUsage, true));
                vmMemoryUsage.get(selectedVm.getId()).add(new MemoryUsageRecord(currentTime + 50, memoryUsage, false));

                printWarmStatus(currentTime, warmFunctions, vmList);

                broker.submitCloudletList(List.of(info.getCloudlet()));
            }

            CloudSim.startSimulation();
            CloudSim.stopSimulation();

            for (CloudletExecutionInfo info : executionSchedule) {
                Cloudlet cloudlet = info.getCloudlet();
                System.out.println("Cloudlet " + cloudlet.getCloudletId() + " executed on VM " + info.vmId +
                        (info.warmStart ? " (warm)" : " (cold)") + ", Finish Time: " + cloudlet.getFinishTime());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Datacenter createDatacenter(String name) throws Exception {
        List<Host> hostList = new ArrayList<>();
        List<Pe> peList = new ArrayList<>();
        peList.add(new Pe(0, new PeProvisionerSimple(1000)));

        int hostId = 0;
        int ram = 2048;
        long storage = 1000000;
        int bw = 10000;

        hostList.add(new Host(
                hostId,
                new RamProvisionerSimple(ram),
                new BwProvisionerSimple(bw),
                storage,
                peList,
                new VmSchedulerTimeShared(peList)
        ));

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                "x86", "Linux", "Xen", hostList, 10.0, 3.0, 0.05, 0.1, 0.1
        );

        return new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), new LinkedList<>(), 0);
    }

    private static DatacenterBroker createBroker() throws Exception {
        return new DatacenterBroker("Broker");
    }

    private static List<CloudletExecutionInfo> createExecutionSchedule(int brokerId) {
        List<CloudletExecutionInfo> schedule = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            long length = 1000;
            int pesNumber = 1;
            long fileSize = 300;
            long outputSize = 300;

            UtilizationModel utilizationModel = new UtilizationModelFull();
            Cloudlet cloudlet = new Cloudlet(i, length, pesNumber, fileSize, outputSize,
                    utilizationModel, utilizationModel, utilizationModel);
            cloudlet.setUserId(brokerId);

            int functionType = i % 3;
            double executionTime = i * 50.0;

            schedule.add(new CloudletExecutionInfo(cloudlet, functionType, executionTime));
        }

        return schedule;
    }

    private static void printWarmStatus(double currentTime, Map<Integer, Map<Integer, Double>> warmFunctions, List<Vm> vmList) {
        System.out.println("Warm function status at time " + currentTime + ":");
        for (Vm vm : vmList) {
            System.out.print("  VM " + vm.getId() + ": ");
            List<String> warmTypes = new ArrayList<>();
            for (Map.Entry<Integer, Map<Integer, Double>> entry : warmFunctions.entrySet()) {
                int funcType = entry.getKey();
                Map<Integer, Double> vmTimes = entry.getValue();
                if (vmTimes.containsKey(vm.getId()) && currentTime - vmTimes.get(vm.getId()) <= WARM_START_DURATION) {
                    warmTypes.add("Func" + funcType);
                }
            }
            System.out.println(warmTypes.isEmpty() ? "No warm functions" : String.join(", ", warmTypes));
        }
    }

    public static class CloudletExecutionInfo {
        private Cloudlet cloudlet;
        private int functionType;
        private double executionTime;
        private int vmId;
        private boolean warmStart;
        private double finishTime;

        public CloudletExecutionInfo(Cloudlet cloudlet, int functionType, double executionTime) {
            this.cloudlet = cloudlet;
            this.functionType = functionType;
            this.executionTime = executionTime;
        }

        public Cloudlet getCloudlet() { return cloudlet; }
        public int getFunctionType() { return functionType; }
        public double getExecutionTime() { return executionTime; }
        public void setVmId(int vmId) { this.vmId = vmId; }
        public void setWarmStart(boolean warmStart) { this.warmStart = warmStart; }
        public void setFinishTime(double finishTime) { this.finishTime = finishTime; }
    }

    public static class MemoryUsageRecord {
        double time;
        double memory;
        boolean allocate;

        public MemoryUsageRecord(double time, double memory, boolean allocate) {
            this.time = time;
            this.memory = memory;
            this.allocate = allocate;
        }

        public double getTime() { return time; }
        public double getMemory() { return memory; }
        public boolean isAllocate() { return allocate; }
    }
}
