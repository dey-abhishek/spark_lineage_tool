"""Method call tracker for inter-procedural analysis.

Tracks method definitions and resolves calls to custom wrapper methods
by mapping arguments to parameters and substituting into method bodies.
"""

import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class MethodDefinition:
    """Represents a method/function definition."""
    name: str
    parameters: List[str]
    defaults: Dict[str, str]  # parameter name -> default value
    body: str
    line_number: int
    language: str  # 'scala' or 'python'


@dataclass
class MethodCall:
    """Represents a method call with arguments."""
    method_name: str
    arguments: List[str]
    line_number: int


class MethodCallTracker:
    """Tracks method definitions and resolves calls."""
    
    def __init__(self):
        self.methods: Dict[str, MethodDefinition] = {}
        self.calls: List[MethodCall] = []
    
    def add_method(self, method: MethodDefinition):
        """Add a method definition."""
        self.methods[method.name] = method
    
    def add_call(self, call: MethodCall):
        """Add a method call."""
        self.calls.append(call)
    
    def resolve_call(self, call: MethodCall) -> Optional[str]:
        """Resolve a method call by substituting arguments into the method body.
        
        Returns the method body with arguments substituted for parameters.
        Uses default values for parameters not provided in the call.
        """
        method = self.methods.get(call.method_name)
        if not method:
            return None
        
        # Handle parameter count mismatch
        if len(call.arguments) > len(method.parameters):
            # Too many arguments, can't resolve
            return None
        
        # Build complete argument list using defaults for missing arguments
        complete_args = []
        for i, param in enumerate(method.parameters):
            if i < len(call.arguments):
                # Use provided argument
                complete_args.append(call.arguments[i])
            elif param in method.defaults:
                # Use default value
                complete_args.append(method.defaults[param])
            else:
                # No argument and no default - this parameter will remain unresolved
                complete_args.append(None)
        
        # Substitute parameters with arguments
        body = method.body
        
        # For Scala, we need to handle string interpolation first, then direct usage
        if method.language == 'scala':
            for param, arg in zip(method.parameters, complete_args):
                if arg is None:
                    # Skip unresolved parameters
                    continue
                    
                # Strip quotes from the argument
                arg_value = arg.strip('"\'')
                
                # Replace $param in string interpolations (s"...$param...")
                # Use the unquoted value for interpolation
                body = re.sub(rf'\${param}\b', arg_value, body)
            
            # Now replace direct parameter usage (but be careful not to replace method names)
            for param, arg in zip(method.parameters, complete_args):
                if arg is None:
                    # Skip unresolved parameters
                    continue
                    
                # Only replace word boundaries, and not if preceded by a dot (method call)
                body = re.sub(rf'(?<![.$])\b{param}\b', arg, body)
                
        elif method.language == 'python':
            for param, arg in zip(method.parameters, complete_args):
                if arg is None:
                    # Skip unresolved parameters
                    continue
                    
                arg_value = arg.strip('"\'')
                # Replace {param} in f-strings
                body = re.sub(rf'\{{{param}\}}', arg_value, body)
                # Replace param in direct usage
                body = re.sub(rf'\b{param}\b', arg, body)
        
        return body
    
    def get_all_resolved_bodies(self) -> List[Tuple[MethodCall, str]]:
        """Resolve all method calls and return (call, resolved_body) pairs."""
        resolved = []
        for call in self.calls:
            body = self.resolve_call(call)
            if body:
                resolved.append((call, body))
        return resolved


class ScalaMethodCallTracker:
    """Extracts and tracks Scala method definitions and calls."""
    
    def __init__(self):
        self.tracker = MethodCallTracker()
    
    def extract_methods(self, content: str) -> List[MethodDefinition]:
        """Extract method definitions from Scala code."""
        methods = []
        
        # Pattern for Scala methods: def methodName(param1: Type1, param2: Type2 = "default"): ReturnType = { body }
        # Also handle single-line methods: def methodName(...) = expression
        method_pattern = re.compile(
            r'def\s+(\w+)\s*\((.*?)\)\s*(?::\s*\w+\s*)?=\s*\{?(.*?)(?:\}|$)',
            re.DOTALL
        )
        
        for match in method_pattern.finditer(content):
            method_name = match.group(1)
            params_str = match.group(2)
            body = match.group(3)
            line_number = content[:match.start()].count('\n') + 1
            
            # Extract parameter names and default values
            parameters = []
            defaults = {}
            
            if params_str.strip():
                # Split by comma, then extract name, type, and default
                for param in params_str.split(','):
                    param = param.strip()
                    if ':' in param:
                        # Split by colon to separate name from type
                        param_name = param.split(':')[0].strip()
                        type_and_default = param.split(':', 1)[1].strip()
                        
                        # Check for default value (= "value" or = value)
                        if '=' in type_and_default:
                            default_value = type_and_default.split('=', 1)[1].strip()
                            defaults[param_name] = default_value
                        
                        parameters.append(param_name)
            
            method = MethodDefinition(
                name=method_name,
                parameters=parameters,
                defaults=defaults,
                body=body,
                line_number=line_number,
                language='scala'
            )
            
            methods.append(method)
            self.tracker.add_method(method)
        
        return methods
    
    def extract_calls(self, content: str) -> List[MethodCall]:
        """Extract method calls from Scala code."""
        calls = []
        
        # Pattern for method calls: methodName("arg1", "arg2") or methodName(arg1, arg2)
        # Handle both object.method and direct calls
        call_pattern = re.compile(
            r'(?:new\s+\w+\([^)]*\)|\w+)\s*\.\s*(\w+)\s*\((.*?)\)',
            re.DOTALL
        )
        
        for match in call_pattern.finditer(content):
            method_name = match.group(1)
            args_str = match.group(2)
            line_number = content[:match.start()].count('\n') + 1
            
            # Extract arguments (handle strings and identifiers)
            arguments = []
            if args_str.strip():
                # Simple split by comma (doesn't handle nested calls perfectly, but good enough)
                for arg in args_str.split(','):
                    arg = arg.strip()
                    # Extract string literals or keep as-is
                    if arg:
                        arguments.append(arg)
            
            # Only track calls if we have a method definition for them
            if method_name in self.tracker.methods:
                call = MethodCall(
                    method_name=method_name,
                    arguments=arguments,
                    line_number=line_number
                )
                calls.append(call)
                self.tracker.add_call(call)
        
        return calls
    
    def get_tracker(self) -> MethodCallTracker:
        """Get the underlying tracker."""
        return self.tracker


class PythonMethodCallTracker:
    """Extracts and tracks Python method definitions and calls."""
    
    def __init__(self):
        self.tracker = MethodCallTracker()
    
    def extract_methods(self, content: str) -> List[MethodDefinition]:
        """Extract method definitions from Python code."""
        methods = []
        
        # Pattern for Python methods: def method_name(self, param1, param2="default"): ... body ...
        # Handle both methods and functions
        method_pattern = re.compile(
            r'def\s+(\w+)\s*\((.*?)\)\s*(?:->.*?)?:\s*\n((?:[ \t]+.*\n)+)',
            re.MULTILINE
        )
        
        for match in method_pattern.finditer(content):
            method_name = match.group(1)
            params_str = match.group(2)
            body = match.group(3)
            line_number = content[:match.start()].count('\n') + 1
            
            # Extract parameter names and default values
            parameters = []
            defaults = {}
            
            if params_str.strip():
                for param in params_str.split(','):
                    param = param.strip()
                    # Remove 'self' or 'cls'
                    if param in ('self', 'cls'):
                        continue
                    
                    # Check for default value (param=value or param: type = value)
                    param_name = param
                    default_value = None
                    
                    # Handle type hints: param: type = value
                    if ':' in param:
                        parts = param.split(':', 1)
                        param_name = parts[0].strip()
                        type_part = parts[1].strip()
                        
                        if '=' in type_part:
                            default_value = type_part.split('=', 1)[1].strip()
                    # Handle simple defaults: param=value
                    elif '=' in param:
                        parts = param.split('=', 1)
                        param_name = parts[0].strip()
                        default_value = parts[1].strip()
                    
                    if param_name:
                        parameters.append(param_name)
                        if default_value:
                            defaults[param_name] = default_value
            
            method = MethodDefinition(
                name=method_name,
                parameters=parameters,
                defaults=defaults,
                body=body,
                line_number=line_number,
                language='python'
            )
            
            methods.append(method)
            self.tracker.add_method(method)
        
        return methods
    
    def extract_calls(self, content: str) -> List[MethodCall]:
        """Extract method calls from Python code."""
        calls = []
        
        # Pattern for method calls: object.method("arg1", "arg2") or method(arg1, arg2)
        call_pattern = re.compile(
            r'(?:\w+)\s*\.\s*(\w+)\s*\((.*?)\)',
            re.DOTALL
        )
        
        for match in call_pattern.finditer(content):
            method_name = match.group(1)
            args_str = match.group(2)
            line_number = content[:match.start()].count('\n') + 1
            
            # Extract arguments
            arguments = []
            if args_str.strip():
                # Simple split by comma
                for arg in args_str.split(','):
                    arg = arg.strip()
                    if arg:
                        arguments.append(arg)
            
            # Only track calls if we have a method definition for them
            if method_name in self.tracker.methods:
                call = MethodCall(
                    method_name=method_name,
                    arguments=arguments,
                    line_number=line_number
                )
                calls.append(call)
                self.tracker.add_call(call)
        
        return calls
    
    def get_tracker(self) -> MethodCallTracker:
        """Get the underlying tracker."""
        return self.tracker

