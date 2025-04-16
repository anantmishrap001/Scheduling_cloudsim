package cloudsim.simulations;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.Arrays;

public class AWS_Lambda_Simulation {

    // Warm start duration in seconds
    private static final double WARM_START_DURATION = 5.0;

    public static void main(String[] args) {
        try {
            int numUsers = 1;
            Calendar calendar = Calendar.getInstance();
            boolean traceFlag = false;

            // Initialize CloudSim
            CloudSim.init(numUsers, calendar, traceFlag);

            // Create Datacenter
            Datacenter datacenter = createDatacenter("Datacenter_0");

            // Create broker
            DatacenterBroker broker = createBroker();
            int brokerId = broker.getId();

            // Create VMs (initially create 2)
            List<Vm> vmList = new ArrayList<>();
            Vm vm1 = new Vm(0, brokerId, 1000, 2, 1024, 10000, 10000, "Xen", new CloudletSchedulerTimeShared());
            Vm vm2 = new Vm(1, brokerId, 1000, 2, 1024, 10000, 10000, "Xen", new CloudletSchedulerTimeShared());
            vmList.add(vm1);
            vmList.add(vm2);

            // Track the next VM ID to use when creating new VMs
            int nextVmId = 2;

            // Create execution schedule with hardcoded timing
            List<CloudletExecutionInfo> executionSchedule = createExecutionSchedule(brokerId);

            // Pre-process all cloudlets and assign VMs based on our Lambda policy
            // Track warm function instances and memory usage
            Map<Integer, Map<Integer, Double>> warmFunctions = new HashMap<>();
            Map<Integer, Boolean> warmStartStatus = new HashMap<>();
            Map<Integer, List<MemoryUsageRecord>> vmMemoryUsage = new HashMap<>();

            for (Vm vm : vmList) {
                vmMemoryUsage.put(vm.getId(), new ArrayList<>());
            }

            // Submit initial VM list
            broker.submitVmList(vmList);

            // Sort the execution schedule by time
            executionSchedule.sort(Comparator.comparingDouble(CloudletExecutionInfo::getExecutionTime));

            // Implementing Round-Robin scheduling
            int currentVmIndex = 0;
            double currentTime = 0.0;

            for (CloudletExecutionInfo info : executionSchedule) {
                Cloudlet cloudlet = info.getCloudlet();
                int functionType = info.getFunctionType();
                currentTime = info.getExecutionTime();  // Simulation time for this execution

                System.out.println("\nTime: " + currentTime + " - Processing function execution for cloudlet " +
                        cloudlet.getCloudletId() + " (Function Type: " + functionType + ")");

                // Round-Robin VM selection logic
                Vm selectedVm = vmList.get(currentVmIndex);
                currentVmIndex = (currentVmIndex + 1) % vmList.size();  // Cycle through VMs

                // Assign selected VM to cloudlet
                cloudlet.setVmId(selectedVm.getId());

                // Update warm function tracking
                warmFunctions.putIfAbsent(functionType, new HashMap<>());
                warmFunctions.get(functionType).put(selectedVm.getId(), currentTime);

                // Set execution time for cloudlet
                double executionLength = warmStartStatus.getOrDefault(cloudlet.getCloudletId(), false) ? 1.0 : 2.0;
                cloudlet.setCloudletLength((long) (executionLength * 500));  // Scale to match expected time

                // Track memory usage (40% RAM usage per cloudlet)
                double memoryUsage = 0.4 * 1024; // 40% of standard RAM
                vmMemoryUsage.get(selectedVm.getId()).add(new MemoryUsageRecord(currentTime, memoryUsage, true));
                vmMemoryUsage.get(selectedVm.getId()).add(new MemoryUsageRecord(currentTime + executionLength, memoryUsage, false));

                // Store additional information
                info.setVmId(selectedVm.getId());
                info.setWarmStart(warmStartStatus.get(cloudlet.getCloudletId()));
                info.setFinishTime(currentTime + executionLength);

                // Print memory usage for this VM
                double currentMemoryUsage = calculateCurrentMemoryUsage(currentTime, vmMemoryUsage.get(selectedVm.getId()));
                double totalMemory = 1024; // Standard VM RAM
                double memoryPercentage = (currentMemoryUsage / totalMemory) * 100;
                System.out.println("VM " + selectedVm.getId() + " memory usage: " +
                        String.format("%.2f", currentMemoryUsage) + "MB / " +
                        totalMemory + "MB (" + String.format("%.1f", memoryPercentage) + "%)");

                // Print VM warmup status
                printWarmStatus(currentTime, warmFunctions, vmList);

                // Print VM memory status
                printMemoryUsage(currentTime, vmMemoryUsage, vmList);
            }

            // Create a list for all the processed cloudlets
            List<Cloudlet> cloudletList = new ArrayList<>();
            for (CloudletExecutionInfo info : executionSchedule) {
                cloudletList.add(info.getCloudlet());
            }

            // Submit all cloudlets to broker
            broker.submitCloudletList(Arrays.asList(cloudletList.toArray(new Cloudlet[0])));

            // Start the simulation
            System.out.println("\nStarting CloudSim simulation...");
            CloudSim.startSimulation();

            // Stop the simulation
            CloudSim.stopSimulation();

            // Print results
            List<Cloudlet> finishedCloudlets = broker.getCloudletReceivedList();
            System.out.println("Simulation completed.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Datacenter createDatacenter(String name) {
        // Implement your Datacenter creation logic here.
        return null;
    }

    private static DatacenterBroker createBroker() throws Exception {
        // Implement your DatacenterBroker creation logic here.
        return new DatacenterBrokerSimple("Broker_0");
    }

    private static List<CloudletExecutionInfo> createExecutionSchedule(int brokerId) {
        // Implement your Cloudlet execution schedule logic here.
        return new ArrayList<>();
    }

    private static double calculateCurrentMemoryUsage(double currentTime, List<MemoryUsageRecord> memoryUsageRecords) {
        // Implement your memory usage calculation here.
        return 0.0;
    }

    private static void printWarmStatus(double currentTime, Map<Integer, Map<Integer, Double>> warmFunctions, List<Vm> vmList) {
        // Implement your warm status printing logic here.
    }

    private static void printMemoryUsage(double currentTime, Map<Integer, List<MemoryUsageRecord>> vmMemoryUsage, List<Vm> vmList) {
        // Implement your memory usage printing logic here.
    }

    // CloudletExecutionInfo class
    private static class CloudletExecutionInfo {
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

        public Cloudlet getCloudlet() {
            return cloudlet;
        }

        public int getFunctionType() {
            return functionType;
        }

        public double getExecutionTime() {
            return executionTime;
        }

        public void setVmId(int vmId) {
            this.vmId = vmId;
        }

        public int getVmId() {
            return vmId;
        }

        public void setWarmStart(Boolean warmStart) {
            this.warmStart = warmStart;
        }

        public void setFinishTime(double finishTime) {
            this.finishTime = finishTime;
        }
    }

    // MemoryUsageRecord class
    private static class MemoryUsageRecord {
        private double timestamp;
        private double memoryUsage;
        private boolean isStart;

        public MemoryUsageRecord(double timestamp, double memoryUsage, boolean isStart) {
            this.timestamp = timestamp;
            this.memoryUsage = memoryUsage;
            this.isStart = isStart;
        }

        public double getTimestamp() {
            return timestamp;
        }

        public double getMemoryUsage() {
            return memoryUsage;
        }

        public boolean isStart() {
            return isStart;
        }
    }
}
