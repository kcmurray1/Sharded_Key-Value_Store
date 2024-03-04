class VectorClock():
    # Simple data structure that functions as a changing-size Vector Clock
    # New addresses can be added via set_address()
    # Can be initialized with a pre-defined dictionary of address - int pairs
    def __init__(self, my_dict=dict()):
        self.vc_dict = my_dict
    
    # Sets the value of address in VC to val
    def set_address(self, address, val=0):
        self.vc_dict[address] = val
    
    # Increments the value stored in the VC at address by 1
    def increment_address(self, address):
        self.vc_dict[address] += 1
    
    # Gets the value stored at address
    def get_address(self, address):
        return self.vc_dict[address]

    # Returns the VC's dictionary
    def get_dictionary(self):
        return self.vc_dict

    # Returns the piecewise maximum of the two Vector Clocks
    def max_with(self, other):
        keys = self.vc_dict.keys()
        for key in keys:
            self.vc_dict[key] = max(self.vc_dict[key], other.vc_dict[key])
        
    # Returns True if both VCs have the same addresses in their views, return False if not
    def has_same_view_as(self, other):
        if len(self.vc_dict) != len(other.vc_dict):
            return False
        
        for address in self.vc_dict:
            if address not in other.vc_dict:
                return False
        
        for address in other.vc_dict:
            if address not in self.vc_dict:
                return False
        
        return True

    # <
    # Condition for lt - Given two VCs A and B:
    # At least one element's value in A must be strictly < the corresponding value in B,
    # and all elements in A must be <= their corresponding elements in B
    def __lt__(self, other):
        # Check if both VCs have the same addresses, return None if not since they cannot be compared
        if not self.has_same_view_as(other):
            return None

        has_strictly_lt_val = False
        for address in other.vc_dict:
            val = other.vc_dict[address]
            if self.vc_dict[address] > val:
                return False
            
            if self.vc_dict[address] < val:
                has_strictly_lt_val = True
        
        return has_strictly_lt_val

    # <= 
    def __le__(self, other):
        if not self.has_same_view_as(other):
            return None   
        return self.__lt__(other) or self.__eq__(other)
    
    # >=
    def __ge__(self, other):
        if not self.has_same_view_as(other):
            return None   
        return self.__gt__(other) or self.__eq__(other)

    # !=
    def __ne__(self, other):
        if not self.has_same_view_as(other):
            return None 
        return self < other or self > other
    
    # >
    def __gt__(self, other):
        if not self.has_same_view_as(other):
            return None

        has_strictly_gt_val = False
        for address in other.vc_dict:
            val = other.vc_dict[address]
            if self.vc_dict[address] < val:
                return False
            
            if self.vc_dict[address] > val:
                has_strictly_gt_val = True
        
        return has_strictly_gt_val

    # ==
    # Checks if all VC addresses have the same exact values - returns True if so, False if not
    def __eq__(self, other):
        if not self.has_same_view_as(other):
            return None
        for addr in self.vc_dict:
            if self.vc_dict[addr] != other.vc_dict[addr]:
                return False
        return True
 
    # |
    # Concurrency check
    # Conditions for concurrency - Given two VCs A and B:
    # A | B if A has at least one element that is strictly < the corresponding element in B,
    # and A also has at least one element that is strictly > the corresponding element in B
    def concurrent_with(self, other):
        if not self.has_same_view_as(other):
            return None
        return not self.__lt__(other) and not self.__gt__(other)
