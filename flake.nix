{
    description = "";
    inputs = {
        nixpkgs.url = "nixpkgs:NixOS/nixos/stable";
    };
    outputs = {self, nixpkgs, inputs, ...}:
    let 
        system = "x86_64-linux";
        pkgs = import nixpkgs { inherit system; };
    in
    {
        pkgs.mkShell {
            buildINputs = [];
            nativeBuildInputs= [];
            shellHook = ``echo "HIII"``;

        }
    };
}
