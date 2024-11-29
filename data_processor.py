import struct

def process_data(registers, data_type, scaling=None, multiplier=1):
    if registers is None:
        return None

    if data_type == 'string':
        # Registers to ASCII string
        serial_number = ''.join(chr(num >> 8) + chr(num & 0xFF) for num in registers if num != 0)
        return serial_number.strip()
    elif data_type == 'int16':
        # 16-bit signed integer
        value = registers[0]
        if value >= 0x8000:
            value -= 0x10000
        if scaling:
            value *= scaling
        return value * multiplier  # Multiplier ekleniyor
    elif data_type == 'uint16':
        # 16-bit unsigned integer
        value = registers[0]
        if scaling:
            value *= scaling
        return value * multiplier  # Multiplier ekleniyor
    elif data_type == 'int32':
        # 32-bit signed integer
        if len(registers) < 2:
            return None
        value_bytes = struct.pack('>HH', registers[0], registers[1])
        value = struct.unpack('>i', value_bytes)[0]
        if scaling:
            value *= scaling
        return value * multiplier  # Multiplier ekleniyor
    elif data_type == 'uint32':
        # 32-bit unsigned integer
        if len(registers) < 2:
            return None
        value_bytes = struct.pack('>HH', registers[0], registers[1])
        value = struct.unpack('>I', value_bytes)[0]
        if scaling:
            value *= scaling
        return value * multiplier  # Multiplier ekleniyor
    elif data_type == 'float32':
        # 32-bit float
        if len(registers) < 2:
            return None
        value_bytes = struct.pack('>HH', registers[0], registers[1])
        value = struct.unpack('>f', value_bytes)[0]
        if scaling:
            value *= scaling
        return value * multiplier  # Multiplier ekleniyor
    else:
        print(f"Desteklenmeyen veri tipi: {data_type}")
        return None
