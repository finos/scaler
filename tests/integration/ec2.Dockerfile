# AMI-baseline for the floci-backed EC2 e2e (see README.md).
#
# floci launches EC2 RunInstances on public.ecr.aws/amazonlinux/amazonlinux:2023, a MINIMAL container image
# that -- unlike a real AL2023 AMI -- lacks base tools the shipped ORB UserData bootstrap relies on, most
# notably `tar` (without which `curl | uv install` fails before anything installs). Layer the missing
# baseline on so the shipped UserData runs unmodified. Built by docker.ensure_ec2_base_image and
# tagged as the base image name, so floci launches this superset in place of the minimal image.
FROM public.ecr.aws/amazonlinux/amazonlinux:2023
RUN dnf install -y tar gzip && dnf clean all
