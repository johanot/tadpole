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
      "${pname}" =
      let
        base = (import ./Cargo.nix {
          pkgs = final;
        });
      in
        base.rootCrate.build.override {
          crateOverrides = final.defaultCrateOverrides // {
            etcd-rs = attrs: {
              nativeBuildInputs = [ final.rustfmt ];
            };
            prost-build = attrs: {
              nativeBuildInputs = [ final.protobuf ];
              PROTOC="${final.protobuf}/bin/protoc";
              PROTOC_INCLUDE="${final.protobuf}/include";
            };
          };
        };
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
