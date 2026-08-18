use std::str::FromStr;

use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Topology {
    Independent,
    Star,
    Balanced {
        branching: usize,
    },
    Chain {
        max_depth: u32,
    },
    Mixed {
        seed: u64,
        root_ppm: u32,
        max_depth: u32,
    },
}

impl Topology {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Independent => "independent",
            Self::Star => "star",
            Self::Balanced { .. } => "balanced",
            Self::Chain { .. } => "chain",
            Self::Mixed { .. } => "mixed",
        }
    }
}

impl FromStr for Topology {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let fields = value.split(':').collect::<Vec<_>>();
        match fields.as_slice() {
            ["independent"] => Ok(Self::Independent),
            ["star"] => Ok(Self::Star),
            ["balanced", branching] => {
                let branching = parse_nonzero(branching, "balanced branching")?;
                Ok(Self::Balanced { branching })
            }
            ["chain", max_depth] => {
                let max_depth = parse_nonzero(max_depth, "chain maximum depth")?;
                Ok(Self::Chain { max_depth })
            }
            ["mixed", seed, root_ppm, max_depth] => {
                let seed = seed
                    .parse::<u64>()
                    .map_err(|error| format!("invalid mixed seed {seed}: {error}"))?;
                let root_ppm = root_ppm
                    .parse::<u32>()
                    .map_err(|error| format!("invalid mixed root share {root_ppm}: {error}"))?;
                if root_ppm > 1_000_000 {
                    return Err(format!("mixed root share exceeds one million: {root_ppm}"));
                }
                let max_depth = parse_nonzero(max_depth, "mixed maximum depth")?;
                Ok(Self::Mixed {
                    seed,
                    root_ppm,
                    max_depth,
                })
            }
            _ => Err(format!(
                "unknown provenance {value}; expected independent, star, balanced:N, chain:N, or mixed:SEED:ROOT_PPM:MAX_DEPTH"
            )),
        }
    }
}

fn parse_nonzero<T>(value: &str, field: &str) -> Result<T, String>
where
    T: FromStr + PartialEq + Default,
    T::Err: std::fmt::Display,
{
    let parsed = value
        .parse::<T>()
        .map_err(|error| format!("invalid {field} {value}: {error}"))?;
    if parsed == T::default() {
        return Err(format!("{field} must be positive"));
    }
    Ok(parsed)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VsetProvenance {
    pub vset: u64,
    pub parent: Option<u64>,
    pub root: u64,
    pub generation: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Provenance {
    pub topology: String,
    pub vset_count: usize,
    pub roots: usize,
    pub max_generation: u32,
    pub nodes: Vec<VsetProvenance>,
}

impl Provenance {
    pub fn build(vset_count: usize, topology: &Topology) -> Result<Self, String> {
        if vset_count == 0 {
            return Err("vset count must be positive".to_owned());
        }
        let nodes = match topology {
            Topology::Independent => independent(vset_count),
            Topology::Star => star(vset_count),
            Topology::Balanced { branching } => balanced(vset_count, *branching),
            Topology::Chain { max_depth } => chain(vset_count, *max_depth),
            Topology::Mixed {
                seed,
                root_ppm,
                max_depth,
            } => mixed(vset_count, *seed, *root_ppm, *max_depth),
        };
        let provenance = Self {
            topology: topology.name().to_owned(),
            vset_count,
            roots: nodes.iter().filter(|node| node.parent.is_none()).count(),
            max_generation: nodes.iter().map(|node| node.generation).max().unwrap_or(0),
            nodes,
        };
        provenance.validate()?;
        Ok(provenance)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.vset_count != self.nodes.len() {
            return Err(format!(
                "declared {} vsets but recorded {} provenance nodes",
                self.vset_count,
                self.nodes.len()
            ));
        }
        for (index, node) in self.nodes.iter().enumerate() {
            let expected = u64::try_from(index + 1).map_err(|error| error.to_string())?;
            if node.vset != expected {
                return Err(format!(
                    "provenance node {index} has vset {}, expected {expected}",
                    node.vset
                ));
            }
            match node.parent {
                None => {
                    if node.root != node.vset || node.generation != 0 {
                        return Err(format!("invalid root provenance for vset {}", node.vset));
                    }
                }
                Some(parent) => {
                    if parent == 0 || parent >= node.vset {
                        return Err(format!(
                            "vset {} has missing, cyclic, or forward parent {parent}",
                            node.vset
                        ));
                    }
                    let parent_index = usize::try_from(parent - 1)
                        .map_err(|error| format!("parent index overflow: {error}"))?;
                    let parent_node = &self.nodes[parent_index];
                    if node.root != parent_node.root
                        || node.generation != parent_node.generation.saturating_add(1)
                    {
                        return Err(format!(
                            "vset {} does not extend parent {parent}'s provenance",
                            node.vset
                        ));
                    }
                }
            }
        }
        let roots = self
            .nodes
            .iter()
            .filter(|node| node.parent.is_none())
            .count();
        let max_generation = self
            .nodes
            .iter()
            .map(|node| node.generation)
            .max()
            .unwrap_or(0);
        if self.roots != roots || self.max_generation != max_generation {
            return Err("provenance summary does not match parent graph".to_owned());
        }
        Ok(())
    }
}

fn independent(vset_count: usize) -> Vec<VsetProvenance> {
    (1..=vset_count)
        .map(|vset| {
            let vset = u64::try_from(vset).expect("vset count fits u64");
            VsetProvenance {
                vset,
                parent: None,
                root: vset,
                generation: 0,
            }
        })
        .collect()
}

fn star(vset_count: usize) -> Vec<VsetProvenance> {
    let mut nodes = Vec::with_capacity(vset_count);
    nodes.push(VsetProvenance {
        vset: 1,
        parent: None,
        root: 1,
        generation: 0,
    });
    for vset in 2..=vset_count {
        nodes.push(VsetProvenance {
            vset: u64::try_from(vset).expect("vset count fits u64"),
            parent: Some(1),
            root: 1,
            generation: 1,
        });
    }
    nodes
}

fn balanced(vset_count: usize, branching: usize) -> Vec<VsetProvenance> {
    let mut nodes = Vec::with_capacity(vset_count);
    nodes.push(VsetProvenance {
        vset: 1,
        parent: None,
        root: 1,
        generation: 0,
    });
    for index in 1..vset_count {
        let parent_index = (index - 1) / branching;
        let parent = &nodes[parent_index];
        nodes.push(VsetProvenance {
            vset: u64::try_from(index + 1).expect("vset count fits u64"),
            parent: Some(parent.vset),
            root: parent.root,
            generation: parent.generation.saturating_add(1),
        });
    }
    nodes
}

fn chain(vset_count: usize, max_depth: u32) -> Vec<VsetProvenance> {
    let width = usize::try_from(max_depth)
        .expect("maximum chain depth fits usize")
        .saturating_add(1);
    let mut nodes = Vec::with_capacity(vset_count);
    for index in 0..vset_count {
        let generation = u32::try_from(index % width).expect("generation fits u32");
        let vset = u64::try_from(index + 1).expect("vset count fits u64");
        if generation == 0 {
            nodes.push(VsetProvenance {
                vset,
                parent: None,
                root: vset,
                generation,
            });
        } else {
            let parent = nodes.last().expect("chain root exists");
            nodes.push(VsetProvenance {
                vset,
                parent: Some(parent.vset),
                root: parent.root,
                generation,
            });
        }
    }
    nodes
}

fn mixed(vset_count: usize, seed: u64, root_ppm: u32, max_depth: u32) -> Vec<VsetProvenance> {
    let mut random = Lcg(seed);
    let mut nodes = Vec::with_capacity(vset_count);
    let mut eligible = Vec::with_capacity(vset_count);
    nodes.push(VsetProvenance {
        vset: 1,
        parent: None,
        root: 1,
        generation: 0,
    });
    eligible.push(0usize);
    for index in 1..vset_count {
        let vset = u64::try_from(index + 1).expect("vset count fits u64");
        let make_root = random.next() % 1_000_000 < u64::from(root_ppm);
        let node = if make_root {
            VsetProvenance {
                vset,
                parent: None,
                root: vset,
                generation: 0,
            }
        } else {
            let candidate = random.next() % u64::try_from(eligible.len()).expect("length fits");
            let parent_index = eligible[usize::try_from(candidate).expect("index fits")];
            let parent = &nodes[parent_index];
            VsetProvenance {
                vset,
                parent: Some(parent.vset),
                root: parent.root,
                generation: parent.generation.saturating_add(1),
            }
        };
        nodes.push(node);
        if nodes[index].generation < max_depth {
            eligible.push(index);
        }
    }
    nodes
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topology_parser_rejects_ambiguous_or_unsafe_values() {
        assert_eq!("star".parse(), Ok(Topology::Star));
        assert!("balanced:0".parse::<Topology>().is_err());
        assert!("chain:0".parse::<Topology>().is_err());
        assert!("mixed:7:1000001:8".parse::<Topology>().is_err());
        assert!("unknown".parse::<Topology>().is_err());
    }

    #[test]
    fn fixed_count_topologies_record_exact_parent_graphs() {
        let independent = Provenance::build(5, &Topology::Independent).expect("independent");
        assert_eq!(independent.roots, 5);
        assert_eq!(independent.max_generation, 0);

        let star = Provenance::build(5, &Topology::Star).expect("star");
        assert_eq!(star.roots, 1);
        assert_eq!(star.max_generation, 1);
        assert!(star.nodes[1..].iter().all(|node| node.parent == Some(1)));

        let balanced =
            Provenance::build(7, &Topology::Balanced { branching: 2 }).expect("balanced tree");
        assert_eq!(balanced.max_generation, 2);
        assert_eq!(balanced.nodes[6].parent, Some(3));

        let chains = Provenance::build(8, &Topology::Chain { max_depth: 2 }).expect("chains");
        assert_eq!(chains.roots, 3);
        assert_eq!(chains.max_generation, 2);
        assert_eq!(chains.nodes[3].parent, None);
    }

    #[test]
    fn mixed_provenance_is_deterministic_and_valid() {
        let topology = Topology::Mixed {
            seed: 17,
            root_ppm: 100_000,
            max_depth: 4,
        };
        let first = Provenance::build(1_000, &topology).expect("mixed fleet");
        let second = Provenance::build(1_000, &topology).expect("mixed fleet");
        assert_eq!(first, second);
        assert!(first.roots > 1);
        assert!(first.max_generation <= 4);
        first.validate().expect("valid explicit parent graph");
    }
}
