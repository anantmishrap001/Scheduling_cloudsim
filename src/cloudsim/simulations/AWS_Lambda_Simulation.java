package cloudsim.simulations;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelStochastic;

import java.util.*;
import java.util.stream.Collectors;

public class AWS_Lambda_Simulation {
    private static Map<Integer, Boolean> warmStartStatus = new HashMap<>();
    private static Map<Integer, List<Cloudlet>> warmFunctions = new HashMap<>();

    public static void main(String[] args) {
        CloudSim simulation = new CloudSim();

        int brokerId = 0;
        Datacenter datacenter = createDatacenter("Datacenter_0");
        DatacenterBroker broker = new DatacenterBroker("Broker_0");
        brokerId = broker.getId();

        List<Vm> vmList = createVms(brokerId);
        List<CloudletExecutionInfo> executionSchedule = createExecutionSchedule(brokerId);

        Map<Integer, List<MemoryUsageRecord>> vmMemoryUsage = new HashMap<>();
        for (Vm vm : vmList) {
            vmMemoryUsage.put(vm.getId(), new ArrayList<>());
        }

        executionSchedule.sort(Comparator.comparingDouble(CloudletExecutionInfo::getExecutionTime));

        for (CloudletExecutionInfo info : executionSchedule) {
            double currentTime = info.getExecutionTime();
            Cloudlet cloudlet = info.getCloudlet();
            Vm selectedVm = vmList.get(info.getVmId() % vmList.size());

            boolean isWarm = warmStartStatus.getOrDefault(cloudlet.getCloudletId(), false);
            double executionLength = isWarm ? 1.0 : 2.0;
            cloudlet.setCloudletLength((long)(executionLength * 500));
            info.setWarmStart(isWarm);

            double memoryUsage = selectedVm.getRam();
            vmMemoryUsage.get(selectedVm.getId()).add(new MemoryUsageRecord(currentTime, memoryUsage, true));
            vmMemoryUsage.get(selectedVm.getId()).add(new MemoryUsageRecord(currentTime + executionLength, memoryUsage, false));

            cloudlet.setVmId(selectedVm.getId());
            broker.submitCloudletList(Collections.singletonList(cloudlet));

            warmStartStatus.put(cloudlet.getCloudletId(), true);
        }

        datacenter.setVmAllocationPolicy(new VmAllocationPolicySimple(new ArrayList<>(datacenter.getHostList())));
        broker.submitVmList(vmList);

        simulation.start();

        printMemoryUsage(simulation.clock(), vmMemoryUsage, vmList);

        for (Cloudlet cloudlet : broker.getCloudletReceivedList()) {
            System.out.printf("Cloudlet %d executed on VM %d with status %s\n",
                    cloudlet.getCloudletId(), cloudlet.getVmId(), cloudlet.getStatus());
        }
    }

    private static Datacenter createDatacenter(String name) {
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
                "x86", "Linux", "Xen", hostList, 10.0, 3.0, 0.05, 0.001, 0.0);

        try {
            return new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), new LinkedList<>(), 0);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static List<Vm> createVms(int brokerId) {
        List<Vm> vmList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            vmList.add(new Vm(i, brokerId, 1000, 1, 512, 1000, 10000, "Xen", new CloudletSchedulerTimeShared()));
        }
        return vmList;
    }

    private static List<CloudletExecutionInfo> createExecutionSchedule(int brokerId) {
        List<CloudletExecutionInfo> schedule = new ArrayList<>();

        UtilizationModel utilizationModel = new UtilizationModelStochastic();

        Cloudlet c1 = new Cloudlet(0, 1000, 1, 1000, 300, utilizationModel, utilizationModel, utilizationModel);
        c1.setUserId(brokerId);
        schedule.add(new CloudletExecutionInfo(c1, 1, 0.0));

        Cloudlet c2 = new Cloudlet(1, 2000, 1, 2000, 300, utilizationModel, utilizationModel, utilizationModel);
        c2.setUserId(brokerId);
        schedule.add(new CloudletExecutionInfo(c2, 2, 2.0));

        return schedule;
    }

    private static void printMemoryUsage(double currentTime, Map<Integer, List<MemoryUsageRecord>> vmMemoryUsage, List<Vm> vmList) {
        System.out.println("Memory Usage at time " + currentTime);
        for (Vm vm : vmList) {
            List<MemoryUsageRecord> records = vmMemoryUsage.get(vm.getId());
            double totalUsage = calculateCurrentMemoryUsage(currentTime, records);
            System.out.printf("VM %d: %.2f MB used\n", vm.getId(), totalUsage);
        }
    }

    private static double calculateCurrentMemoryUsage(double currentTime, List<MemoryUsageRecord> memoryRecords) {
        double usage = 0.0;
        for (MemoryUsageRecord record : memoryRecords) {
            if (record.isStart() && record.getTime() <= currentTime) {
                usage += record.getMemoryUsed();
            } else if (!record.isStart() && record.getTime() <= currentTime) {
                usage -= record.getMemoryUsed();
            }
        }
        return usage;
    }
}

class CloudletExecutionInfo {
    private Cloudlet cloudlet;
    private int vmId;
    private double executionTime;
    private boolean isWarmStart;

    public CloudletExecutionInfo(Cloudlet cloudlet, int vmId, double executionTime) {
        this.cloudlet = cloudlet;
        this.vmId = vmId;
        this.executionTime = executionTime;
    }

    public Cloudlet getCloudlet() {
        return cloudlet;
    }

    public int getVmId() {
        return vmId;
    }

    public double getExecutionTime() {
        return executionTime;
    }

    public void setWarmStart(boolean warmStart) {
        this.isWarmStart = warmStart;
    }

    public boolean isWarmStart() {
        return isWarmStart;
    }
}

class MemoryUsageRecord {
    private double time;
    private double memoryUsed;
    private boolean isStart;

    public MemoryUsageRecord(double time, double memoryUsed, boolean isStart) {
        this.time = time;
        this.memoryUsed = memoryUsed;
        this.isStart = isStart;
    }

    public double getTime() {
        return time;
    }

    public double getMemoryUsed() {
        return memoryUsed;
    }

    public boolean isStart() {
        return isStart;
    }
}