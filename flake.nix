{
  description = "Flake file for the FSSServer";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs =
    { self, nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        buildInputs = with pkgs; [
          openssl
          pkg-config
        ];
        nativeBuildInputs = with pkgs; [
          postgresql
          websocat
          redis
          pgcli
        ];
        shellHook = ''echo "Welcome!"'';
      };

      defaultPackage.x86_64-linux = pkgs.rustPlatform.buildRustPackage {
        nativeBuildInputs = with pkgs; [
          pkg-config
        ];
        buildInputs = with pkgs; [
          openssl
        ];
        pname = "fsss";
        src = ./.;
        version = "0.1.0";
        cargoLock = {
          lockFile = ./Cargo.lock;
        };
      };
    };
}
