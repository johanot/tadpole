{
  description = "tadpole";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-21.11";
  };

  outputs = { self, nixpkgs }:
  let
    pname = "tadpole";
    system = "x86_64-linux";
    pkgs = import nixpkgs {
      inherit system;
      overlays = [ self.overlay ];
    };
  in {
    packages.${system}.${pname} = pkgs.${pname};
    defaultPackage.${system} = pkgs.${pname};

    overlay = final: prev: {
      "${pname}" = (import ./Cargo.nix {
        pkgs = final;
      }).rootCrate.build;
    };

    devShell.${system} = with pkgs; mkShell {
      buildInputs = [
        cargo
        crate2nix
        openssl.dev
        pkgconfig
        rustc
        rustfmt
        protobuf
      ];
    };
    PROTOC="${pkgs.protobuf}/bin/protoc";
    PROTOC_INCLUDE="${pkgs.protobuf}/include";
  };
}
