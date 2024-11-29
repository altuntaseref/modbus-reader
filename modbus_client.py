from pymodbus.client import ModbusTcpClient

class ModbusClient:
    def __init__(self, host, port, slave_id):
        self.client = ModbusTcpClient(host=host, port=port)
        self.slave_id = slave_id

    def connect(self):
        return self.client.connect()

    def close(self):
        self.client.close()

    def read_registers(self, address, count):
        response = self.client.read_holding_registers(address=address, count=count, slave=self.slave_id)
        if not response.isError():
            return response.registers
        else:
            print(f"Error reading registers at address {address}")
            return None
