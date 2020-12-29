{
    description = "go-kafka-protobuf";

    inputs = {
      nixpkgs.url = "github:nixos/nixpkgs/nixos-20.09";
    };

    outputs = { self, nixpkgs }: let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
    in {
      devShell.${system} = pkgs.mkShell {
        buildInputs = with pkgs; [
          go_1_15
          gopls
          go-outline
          gocode
          gocode-gomod
          gopkgs
          godef
          golint
          delve
          
          go-protobuf
          protobuf
        ];

        GO111MODULE = "on";
        GOPRIVATE = "github.com/xtruder/*";
        GOFLAGS = "-tags=postgres";
        CGO_ENABLED = "1";

        hardeningDisable = [ "all" ];

        shellHook = ''
          export PATH=$PWD/node_modules/.bin:~/go/bin:$PATH
          export POSTGRESQL_URL=postgres://user:password@postgres:5432/app?sslmode=disable
          export SCHEMA_REGISTRY_URL=http://schema-registry:8081
        '';
      };
    };
}