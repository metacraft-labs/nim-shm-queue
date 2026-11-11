{
  description = "nim-shm-queue - layered shared-memory MPSC queue library for Nim";

  inputs = {
    nixos-modules.url = "github:metacraft-labs/devops-modules";
    nixpkgs.follows = "nixos-modules/nixpkgs-unstable";
    flake-parts.follows = "nixos-modules/flake-parts";
    git-hooks.follows = "nixos-modules/git-hooks-nix";
  };

  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      perSystem =
        { pkgs, ... }:
        let
          version = builtins.replaceStrings [ "\n" "\r" ] [ "" "" ] (builtins.readFile ./version.txt);
        in
        {
          # The dev env `repro.nim`'s `devEnv` activates (`useFlakeDevShell()`)
          # and CI enters through setup-dev-env. It must stand on its own: CI
          # checks this repository out without the workspace around it.
          #
          # `mkShell`'s stdenv supplies the C back-end `nim c` shells out to
          # (gcc on Linux, clang on macOS), matching the `uses:` floor in
          # `repro.nim`.
          #
          # No shellHook installs git hooks. The rules live in the committed
          # `.pre-commit-config.yaml` (metacraft-dev-guidelines
          # repo-requirements.md section 4.1: the flake provides the tools, the
          # committed config provides the rules), installed with
          # `prek install`. The shell only provides the tools that config runs:
          # its `language: system` hooks and `uv`, without which prek cannot
          # build the python hook environments on NixOS.
          devShells.default = pkgs.mkShell {
            packages = with pkgs; [
              nim
              just
              git
              # pre-commit tooling for the committed config
              prek
              uv
              editorconfig-checker
              nixfmt
              prettier
            ];
          };

          # The library is source: consumers put `src/` on their Nim path.
          packages.default = pkgs.stdenvNoCC.mkDerivation {
            pname = "nim-shm-queue";
            inherit version;
            src = ./.;
            dontBuild = true;
            installPhase = ''
              mkdir -p $out
              cp -R src shm_queue.nimble version.txt README.md LICENSE $out/
            '';
          };
        };
    };
}
