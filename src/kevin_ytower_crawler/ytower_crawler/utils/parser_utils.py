import re

def extract_weight_unit(quantity_str: str) -> tuple[str, str]:
    """
    Splits a raw quantity string (e.g., '600g', '1/2 cup') into (weight, unit).
    """
    quantity_str = quantity_str.strip()
    
    # Regex: (group 1: digits/dots/slashes/spaces) (group 2: everything else)
    match = re.match(r'([\d\./\s]+)(.*)', quantity_str)
    
    if match:
        weight = match.group(1).strip()
        unit = match.group(2).strip()
        
        # Handle cases where no number was matched (e.g., "A pinch")
        if not weight and unit:
            return '', unit
        if not unit and re.search(r'[a-zA-Z\u4e00-\u9fa5]', weight):
             return '', weight
             
        return weight, unit
    
    # Fallback: No match, return original string as unit.
    return '', quantity_str