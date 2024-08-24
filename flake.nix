
{

  # Inputs omitted for brevity

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, flake-utils, ... }: 
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell { 
          packages = with pkgs; [ uv python312Full ]; 

        shellHook = ''

          test -d venv || uv venv venv
          source venv/bin/activate
          
          echo "Activated venv"

        '';
        };
      }
    );

}
